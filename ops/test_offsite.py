import copy
import datetime as dt
import hashlib
import http.server
import json
import threading
import time
import unittest
import offsite as o

KEY=b"test-only-catalog-hmac-key-32-bytes"


class MemoryS3:
    def __init__(self,objects=None):
        self.objects=dict(objects or {})
        self.puts=[]
    def listing(self,prefix=""):
        for key,value in sorted(self.objects.copy().items()):
            if key.startswith(prefix):
                yield {"key":key,"size":len(value),"modified":"2020-01-01T00:00:00Z"}
    def request(self,method,key="",data=b"",**_):
        if method=="GET":return self.objects[key]
        if method=="PUT":self.puts.append(key);self.objects[key]=data;return b""
        if method=="DELETE":self.objects.pop(key,None);return b""
        raise AssertionError(method)


class OffsiteTests(unittest.TestCase):
    def test_real_source_deletion_and_mutation_do_not_change_a_committed_generation(self):
        source=MemoryS3({"backupstore/volume/backup.cfg":b"original snapshot metadata","backupstore/blocks/ab":b"original encrypted disk block"})
        saved=copy.deepcopy(source.objects);destination=MemoryS3()
        generation,catalog=o.copy_generation(source,destination,KEY,{"backups":[{"id":"test"}]})
        source.objects.clear()
        restored=MemoryS3()
        metadata=o.restore_generation(destination,restored,generation,KEY)
        self.assertEqual(restored.objects,saved)
        self.assertEqual(metadata["backups"],[{"id":"test"}])
        self.assertEqual(catalog["source_bytes"],sum(map(len,saved.values())))
        self.assertFalse(any(k.startswith("leases/") for k in destination.objects))

    def test_shared_blocks_are_deduplicated_and_rechecked_before_commit(self):
        source=MemoryS3({"block":b"content"});destination=MemoryS3()
        o.copy_generation(source,destination,KEY,{})
        o.copy_generation(source,destination,KEY,{})
        self.assertEqual(sum(p.startswith("objects/") for p in destination.puts),1)
        catalogs=set(destination.objects)
        digest=hashlib.sha256(b"content").hexdigest()
        destination.objects["objects/"+digest]=b"corrupt"
        with self.assertRaisesRegex(RuntimeError,"verification"):
            o.copy_generation(source,destination,KEY,{})
        self.assertEqual(set(destination.objects),catalogs,"an invalid reused block must not produce a recovery point")

    def test_unauthenticated_catalog_never_writes_to_recovery_target(self):
        source=MemoryS3({"block":b"content"});destination=MemoryS3()
        generation,_=o.copy_generation(source,destination,KEY,{})
        value=json.loads(destination.objects[generation]);value["catalog"]["objects"][0]["size"]+=1
        destination.objects[generation]=json.dumps(value).encode()
        target=MemoryS3()
        with self.assertRaisesRegex(RuntimeError,"authentication"):
            o.restore_generation(destination,target,generation,KEY)
        self.assertEqual(target.objects,{})

    def test_disaster_restore_refuses_an_existing_bucket_and_enforces_capacity(self):
        source=MemoryS3({"block":b"content"});destination=MemoryS3()
        generation,_=o.copy_generation(source,destination,KEY,{})
        target=MemoryS3({"user-object":b"must-survive"})
        with self.assertRaisesRegex(RuntimeError,"empty"):
            o.restore_generation(destination,target,generation,KEY)
        self.assertEqual(target.objects,{"user-object":b"must-survive"})
        with self.assertRaisesRegex(RuntimeError,"capacity"):
            o.copy_generation(source,MemoryS3(),KEY,{},max_destination_bytes=1)

    def test_retention_keeps_active_restore_and_shared_live_blocks(self):
        destination=MemoryS3();generations=[]
        for index in range(4):
            source=MemoryS3({"block":str(index).encode(),"shared":b"keep"})
            generation,_=o.copy_generation(source,destination,KEY,{"created_at":str(index)})
            generations.append(generation)
            # Set a deterministic authenticated ordering independent of clock resolution.
            value=o.decode_catalog(destination.objects[generation],KEY);value["created_at"]=str(index)
            destination.objects[generation]=o.encode_catalog(value,KEY)
        lease={"schema_version":1,"objects":[],"catalog_key":generations[0],"expires_at":int(time.time())+600}
        destination.objects["leases/active.json"]=o.encode_catalog(lease,KEY)
        o.prune_catalogs(destination,KEY,keep=2)
        self.assertIn(generations[0],destination.objects)
        self.assertNotIn(generations[1],destination.objects)
        self.assertIn(generations[2],destination.objects)
        self.assertIn(generations[3],destination.objects)
        restored=MemoryS3()
        o.restore_generation(destination,restored,generations[0],KEY)
        self.assertEqual(restored.objects,{"block":b"0","shared":b"keep"})

    def test_credentials_are_not_forwarded_on_an_s3_redirect(self):
        received=[]
        class Receiver(http.server.BaseHTTPRequestHandler):
            def do_GET(self):received.append(self.headers.get("Authorization"));self.send_response(200);self.end_headers()
            def log_message(self,*_):pass
        receiver=http.server.ThreadingHTTPServer(("127.0.0.1",0),Receiver)
        class Redirect(http.server.BaseHTTPRequestHandler):
            def do_GET(self):
                self.send_response(302);self.send_header("Location","http://127.0.0.1:"+str(receiver.server_port)+"/leak");self.end_headers()
            def log_message(self,*_):pass
        redirect=http.server.ThreadingHTTPServer(("127.0.0.1",0),Redirect)
        for server in (receiver,redirect):threading.Thread(target=server.serve_forever,daemon=True).start()
        try:
            client=o.S3({"endpoint":"http://127.0.0.1:"+str(redirect.server_port),"bucket":"test-only","access_key":"test-key","secret_key":"test-secret","region":"auto","private_source":True})
            with self.assertRaisesRegex(RuntimeError,"302"):client.request("GET")
            self.assertEqual(received,[])
        finally:
            for server in (receiver,redirect):server.shutdown();server.server_close()


if __name__=="__main__":unittest.main()
