#!/usr/bin/env python3
"""Copy a private Longhorn backup store to an independent S3 failure domain.

The destination is content addressed; a committed catalog references verified
objects only. Restoring materializes a catalog into an EMPTY replacement bucket.
Credentials and catalog signing keys are read from projected files, never argv.
No bucket creation, public ACL, or source deletion is performed.
"""
import argparse
import datetime as dt
import hashlib
import hmac
import json
import os
import pathlib
import re
import time
import urllib.error
import urllib.parse
import urllib.request
import xml.etree.ElementTree as ET

from capacity import Kube, NS

GIB = 1024**3
MAX_OBJECT = 64*1024**2
HASH = re.compile(r"^[0-9a-f]{64}$")
LOCK = "flash.heterocloud.io/offsite-lock-until"
LOCK_OWNER = "flash.heterocloud.io/offsite-lock-owner"


class NoRedirect(urllib.request.HTTPRedirectHandler):
    def redirect_request(self, *_):
        return None


def canonical_query(query):
    return "&".join(urllib.parse.quote(str(k), safe="")+"="+urllib.parse.quote(str(v), safe="") for k,v in sorted(query.items()))


class S3:
    def __init__(self, config, opener=None):
        self.config = config
        origin = urllib.parse.urlsplit(config["endpoint"])
        if origin.username or origin.password or origin.query or origin.fragment or origin.path not in ("","/"):
            raise ValueError("S3 endpoint must be an origin without credentials or a path")
        if origin.scheme != "https" and not config.get("private_source"):
            raise ValueError("off-cluster S3 transport requires HTTPS")
        if not re.fullmatch(r"[a-z0-9][a-z0-9.-]{1,61}[a-z0-9]", config["bucket"]):
            raise ValueError("invalid private bucket name")
        self.origin = origin
        self.opener = opener or urllib.request.build_opener(urllib.request.ProxyHandler({}), NoRedirect())

    def request(self, method, key="", data=b"", query=None, optional=False):
        query = query or {}
        now = dt.datetime.now(dt.timezone.utc)
        date, timestamp = now.strftime("%Y%m%d"), now.strftime("%Y%m%dT%H%M%SZ")
        path = "/"+self.config["bucket"]+("/"+urllib.parse.quote(key,safe="/-_.~") if key else "")
        qs = canonical_query(query)
        scope = date+"/"+self.config.get("region","auto")+"/s3/aws4_request"
        payload_hash = hashlib.sha256(data).hexdigest()
        headers = {"host":self.origin.netloc, "x-amz-content-sha256":payload_hash, "x-amz-date":timestamp}
        header_names = ";".join(sorted(headers))
        canonical_headers = "".join(k+":"+headers[k]+"\n" for k in sorted(headers))
        canonical = "\n".join([method,path,qs,canonical_headers,header_names,payload_hash])
        to_sign = "AWS4-HMAC-SHA256\n"+timestamp+"\n"+scope+"\n"+hashlib.sha256(canonical.encode()).hexdigest()
        key_bytes = ("AWS4"+self.config["secret_key"]).encode()
        for part in (date,self.config.get("region","auto"),"s3","aws4_request"):
            key_bytes = hmac.new(key_bytes,part.encode(),hashlib.sha256).digest()
        signature = hmac.new(key_bytes,to_sign.encode(),hashlib.sha256).hexdigest()
        headers["Authorization"] = "AWS4-HMAC-SHA256 Credential="+self.config["access_key"]+"/"+scope+", SignedHeaders="+header_names+", Signature="+signature
        request = urllib.request.Request(urllib.parse.urlunsplit((self.origin.scheme,self.origin.netloc,path,qs,"")),data=data if method in ("PUT","POST") else None,method=method,headers=headers)
        try:
            with self.opener.open(request,timeout=30) as response:
                # Redirecting signed credentials is never permitted.
                if response.geturl() != request.full_url:
                    raise RuntimeError("S3 redirect refused")
                body=response.read(MAX_OBJECT+1)
                if len(body)>MAX_OBJECT:
                    raise RuntimeError("backup object exceeds bounded transfer size")
                return body
        except urllib.error.HTTPError as error:
            if optional and error.code==404: return None
            raise RuntimeError("private_backup_storage_http_"+str(error.code)) from None
        except urllib.error.URLError:
            raise RuntimeError("private_backup_storage_unavailable") from None

    def listing(self,prefix=""):
        token=None
        while True:
            query={"list-type":"2","prefix":prefix,"max-keys":"1000"}
            if token:query["continuation-token"]=token
            root=ET.fromstring(self.request("GET",query=query))
            ns={"s":"http://s3.amazonaws.com/doc/2006-03-01/"}
            for item in root.findall("s:Contents",ns):
                yield {"key":item.findtext("s:Key",namespaces=ns),"size":int(item.findtext("s:Size",namespaces=ns)),
                       "modified":item.findtext("s:LastModified",namespaces=ns)}
            if root.findtext("s:IsTruncated",namespaces=ns)!="true":break
            token=root.findtext("s:NextContinuationToken",namespaces=ns)
            if not token:raise RuntimeError("S3 continuation token is missing")


def encode_catalog(catalog, key):
    body=json.dumps(catalog,sort_keys=True,separators=(",",":")).encode()
    return json.dumps({"catalog":catalog,"hmac_sha256":hmac.new(key,body,hashlib.sha256).hexdigest()},sort_keys=True,separators=(",",":")).encode()


def decode_catalog(body, key):
    value=json.loads(body)
    canonical=json.dumps(value["catalog"],sort_keys=True,separators=(",",":")).encode()
    signature=hmac.new(key,canonical,hashlib.sha256).hexdigest()
    if not hmac.compare_digest(signature,value.get("hmac_sha256","")):
        raise RuntimeError("backup catalog authentication failed")
    catalog=value["catalog"]
    if catalog.get("schema_version")!=1 or not isinstance(catalog.get("objects"),list):
        raise RuntimeError("unsupported backup catalog")
    for obj in catalog["objects"]:
        if not HASH.fullmatch(obj.get("sha256","")) or not isinstance(obj.get("key"),str) or not 0<=obj.get("size",-1)<=MAX_OBJECT:
            raise RuntimeError("invalid object in authenticated catalog")
    return catalog


def copy_generation(source, destination, key, catalog_metadata, max_bytes=128*GIB, renew=lambda:None, max_destination_bytes=512*GIB):
    records=[]
    total=0
    inventory=list(destination.listing("objects/"))
    existing={item["key"] for item in inventory}
    destination_bytes=sum(item["size"] for item in inventory)
    for item in source.listing():
        renew()
        if item["size"]>MAX_OBJECT:raise RuntimeError("source object too large")
        total+=item["size"]
        if total>max_bytes:raise RuntimeError("offsite source capacity limit exceeded")
        body=source.request("GET",item["key"])
        if len(body)!=item["size"]:raise RuntimeError("source changed during export")
        digest=hashlib.sha256(body).hexdigest()
        name="objects/"+digest
        if name not in existing:
            if destination_bytes+len(body)>max_destination_bytes:
                raise RuntimeError("offsite destination capacity limit exceeded")
            destination.request("PUT",name,body)
            destination_bytes+=len(body)
            existing.add(name)
        # Verify reused content too, before committing a new recovery point.
        if hashlib.sha256(destination.request("GET",name)).hexdigest()!=digest:
            raise RuntimeError("offsite object verification failed")
        records.append(dict(key=item["key"],size=len(body),sha256=digest))
    catalog=dict(catalog_metadata,schema_version=1,objects=records,source_bytes=total,created_at=dt.datetime.now(dt.timezone.utc).isoformat())
    generation="catalogs/"+dt.datetime.now(dt.timezone.utc).strftime("%Y%m%dT%H%M%S")+"-"+os.urandom(4).hex()+".json"
    payload=encode_catalog(catalog,key)
    destination.request("PUT",generation,payload)
    decode_catalog(destination.request("GET",generation),key)
    return generation,catalog


def restore_generation(source, destination, catalog_key, signing_key):
    if not catalog_key.startswith("catalogs/") or not catalog_key.endswith(".json"):
        raise RuntimeError("an explicit committed catalog is required")
    catalog=decode_catalog(source.request("GET",catalog_key),signing_key)
    if next(destination.listing(),None) is not None:
        raise RuntimeError("disaster restore requires an empty replacement bucket")
    lease="leases/restore-"+os.urandom(16).hex()+".json"
    def renew():
        source.request("PUT",lease,encode_catalog({"schema_version":1,"objects":[],"catalog_key":catalog_key,"expires_at":int(time.time())+180},signing_key))
    try:
        renew()
        for item in catalog["objects"]:
            renew()
            body=source.request("GET","objects/"+item["sha256"])
            if len(body)!=item["size"] or hashlib.sha256(body).hexdigest()!=item["sha256"]:
                raise RuntimeError("offsite restore hash mismatch")
            destination.request("PUT",item["key"],body)
            if hashlib.sha256(destination.request("GET",item["key"])).hexdigest()!=item["sha256"]:
                raise RuntimeError("replacement store hash mismatch")
    finally:
        source.request("DELETE",lease)
    # The operator imports this verified tenant/CSI catalog only after restoring
    # the control-plane database and configuring the new private backup target.
    return {k:v for k,v in catalog.items() if k!="objects"}


def prune_catalogs(destination, signing_key, keep=7, now=None):
    """Retain authenticated recovery points and pin any active disaster restore.

    Content blocks have an additional two-day grace. A failed export cannot
    delete the last known good generation; GC runs independently before copying.
    """
    if keep<2:raise ValueError("at least two offsite recovery points are required")
    now=time.time() if now is None else now
    active=set()
    for lease in destination.listing("leases/"):
        value=decode_catalog(destination.request("GET",lease["key"]),signing_key)
        if value.get("expires_at",0)>now:
            active.add(value.get("catalog_key"))
        else:
            destination.request("DELETE",lease["key"])
    catalogs=[]
    for item in destination.listing("catalogs/"):
        value=decode_catalog(destination.request("GET",item["key"]),signing_key)
        catalogs.append((value.get("created_at",""),item["key"],value))
    catalogs.sort(reverse=True)
    live=set()
    for index,(_,name,value) in enumerate(catalogs):
        if index<keep or name in active:
            live.update(item["sha256"] for item in value["objects"])
        else:
            destination.request("DELETE",name)
    for item in destination.listing("objects/"):
        digest=item["key"].removeprefix("objects/")
        modified=item.get("modified")
        if not HASH.fullmatch(digest) or digest in live or not modified:continue
        age=now-dt.datetime.fromisoformat(modified.replace("Z","+00:00")).timestamp()
        if age>2*86400:
            destination.request("DELETE",item["key"])


class SourceLock:
    def __init__(self,kube):
        self.kube=kube
        self.base=f"/apis/flash.heterocloud.io/v1alpha1/namespaces/{NS}/flashbackups"
        self.backups=[]
        self.last_renew=0
        self.identity=os.environ.get("HOSTNAME","offsite")+"-"+os.urandom(8).hex()
        self.lease=f"/apis/coordination.k8s.io/v1/namespaces/{os.environ.get('POD_NAMESPACE','heterocloud-flash')}/leases/flash-offsite-backup"

    def acquire(self):
        self.renew_lease()
        self.backups=[b for b in self.kube.call(self.base)["items"] if b.get("status",{}).get("phase")=="ready" and not b["metadata"].get("deletionTimestamp")]
        self.renew(force=True)

    def renew(self,force=False):
        if not force and time.time()-self.last_renew<30:return
        self.renew_lease()
        until=dt.datetime.fromtimestamp(time.time()+180,dt.timezone.utc).isoformat()
        for backup in self.backups:
            path=self.base+"/"+backup["metadata"]["name"]
            current=self.kube.call(path)
            if current["metadata"]["uid"]!=backup["metadata"]["uid"] or current["metadata"].get("deletionTimestamp"):
                raise RuntimeError("source backup changed during export")
            current["metadata"].setdefault("annotations",{})[LOCK]=until
            current["metadata"]["annotations"][LOCK_OWNER]=self.identity
            self.kube.call(path,"PUT",current)
        self.last_renew=time.time()

    def renew_lease(self):
        try:
            value=self.kube.call(self.lease)
        except urllib.error.HTTPError as error:
            if error.code!=404:raise
            value={"apiVersion":"coordination.k8s.io/v1","kind":"Lease","metadata":{"name":"flash-offsite-backup"},"spec":{}}
        spec=value["spec"]
        previous=dt.datetime.fromisoformat(spec.get("renewTime","1970-01-01T00:00:00Z").replace("Z","+00:00")).timestamp()
        if spec.get("holderIdentity") not in (None,self.identity) and time.time()-previous<180:
            raise RuntimeError("another offsite export is running")
        spec.update(holderIdentity=self.identity,leaseDurationSeconds=180,renewTime=dt.datetime.now(dt.timezone.utc).isoformat())
        self.kube.call(self.lease if value["metadata"].get("resourceVersion") else self.lease.rsplit("/",1)[0],
                       "PUT" if value["metadata"].get("resourceVersion") else "POST",value)

    def metadata(self):
        # No application environment, secret bindings, credentials or Pod specs.
        backups=[{"metadata":{"name":b["metadata"]["name"]},"spec":b["spec"],"status":b.get("status",{})} for b in self.backups]
        snapshots=self.kube.call(f"/apis/snapshot.storage.k8s.io/v1/namespaces/{NS}/volumesnapshots")["items"]
        snapshot_names={b["metadata"]["name"] for b in self.backups}
        snapshots=[{"metadata":{"name":s["metadata"]["name"]},"spec":s["spec"],"status":s.get("status",{})} for s in snapshots if s["metadata"]["name"] in snapshot_names]
        contents=[]
        for snapshot in snapshots:
            name=snapshot.get("status",{}).get("boundVolumeSnapshotContentName")
            if name:
                content=self.kube.call("/apis/snapshot.storage.k8s.io/v1/volumesnapshotcontents/"+name)
                contents.append({"metadata":{"name":name},"spec":content["spec"],"status":content.get("status",{})})
        return {"backups":backups,"snapshots":snapshots,"snapshot_contents":contents}

    def release(self):
        for backup in self.backups:
            try:
                path=self.base+"/"+backup["metadata"]["name"]
                current=self.kube.call(path)
                if current["metadata"]["uid"]!=backup["metadata"]["uid"]:continue
                annotations=current["metadata"].get("annotations",{})
                if annotations.get(LOCK_OWNER)!=self.identity:continue
                annotations.pop(LOCK,None)
                annotations.pop(LOCK_OWNER,None)
                self.kube.call(path,"PUT",current)
            except (OSError,RuntimeError):
                pass # The bounded lease expires even after a worker crash.
        try:
            value=self.kube.call(self.lease)
            if value["spec"].get("holderIdentity")==self.identity:
                value["spec"]["renewTime"]="1970-01-01T00:00:00Z"
                self.kube.call(self.lease,"PUT",value)
        except (OSError,ValueError,KeyError):
            pass


def read_config(path):
    value=json.loads(pathlib.Path(path).read_text())
    for required in ("endpoint","bucket","access_key","secret_key"):
        if not value.get(required):raise RuntimeError("backup credential configuration is incomplete")
    return value


def main():
    parser=argparse.ArgumentParser(description=__doc__)
    parser.add_argument("command",choices=["copy","restore","list"])
    parser.add_argument("--source-config",default="/source/config.json")
    parser.add_argument("--destination-config",default="/destination/config.json")
    parser.add_argument("--signing-key-file",default="/destination/catalog-hmac-key")
    parser.add_argument("--catalog")
    parser.add_argument("--max-source-gib",type=int,default=128)
    parser.add_argument("--max-destination-gib",type=int,default=512)
    parser.add_argument("--metadata-output")
    parser.add_argument("--keep-catalogs",type=int,default=7)
    args=parser.parse_args()
    try:
        key=pathlib.Path(args.signing_key_file).read_bytes()
        if len(key)<32:raise RuntimeError("catalog signing key must contain at least 32 random bytes")
        source=S3(read_config(args.source_config))
        destination=S3(read_config(args.destination_config))
        if (source.origin.netloc,source.config["bucket"])==(destination.origin.netloc,destination.config["bucket"]):
            raise RuntimeError("source and disaster recovery store must be independent")
        if args.command=="restore":
            if not args.catalog:raise RuntimeError("select a committed backup catalog")
            result=restore_generation(source,destination,args.catalog,key)
            if args.metadata_output:
                with os.fdopen(os.open(args.metadata_output,os.O_WRONLY|os.O_CREAT|os.O_EXCL,0o600),"w") as output:
                    json.dump(result,output,indent=2)
            print(json.dumps({"restored":True,"objects_verified":True,"backups":len(result.get("backups",[]))}))
        elif args.command=="list":
            print(json.dumps({"catalogs":[i["key"] for i in source.listing("catalogs/")]}))
        else:
            lock=SourceLock(Kube())
            try:
                lock.acquire()
                prune_catalogs(destination,key,args.keep_catalogs)
                generation,catalog=copy_generation(source,destination,key,lock.metadata(),args.max_source_gib*GIB,lock.renew,args.max_destination_gib*GIB)
                prune_catalogs(destination,key,args.keep_catalogs)
                print(json.dumps({"copied":True,"catalog":generation,"objects":len(catalog["objects"]),"source_bytes":catalog["source_bytes"]}))
            finally:
                lock.release()
    except (OSError,ValueError,KeyError,RuntimeError,ET.ParseError):
        # Exceptions from urllib can contain credential-bearing endpoint details.
        raise SystemExit("Offsite backup did not complete. Check the protected destination configuration and last committed catalog.") from None


if __name__=="__main__":
    main()
