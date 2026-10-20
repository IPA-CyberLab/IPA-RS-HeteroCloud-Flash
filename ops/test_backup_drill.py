import copy
import datetime as dt
import json
import unittest
import urllib.error

import backup_drill as d


class VerificationSecurity(unittest.TestCase):
    def setup_fixture(self, legacy=False):
        backup={"metadata":{"name":"backup-test","uid":"backup-owner","annotations":{d.REQUEST:"test",d.RESULT:json.dumps({"phase":"restoring","started_at":d.stamp(),"volume_name":"clone-pv","volume_handle":"clone-volume"})}},
                "spec":{"id":"11111111-1111-4111-8111-111111111111","service_instance_id":"22222222-2222-4222-8222-222222222222","volume_size_bytes":1024**3},"status":{"phase":"ready"}}
        name,pvc,job=d.names(backup)
        metadata={"name":name,"uid":"job-uid","labels":{d.LABEL:backup["spec"]["id"]},"ownerReferences":[{"uid":"backup-owner"}]}
        objects={pvc:{"metadata":metadata,"spec":{"volumeName":"clone-pv"},"status":{"phase":"Bound"}},
                 "/api/v1/persistentvolumes/clone-pv":{"spec":{"csi":{"driver":"driver.longhorn.io","volumeHandle":"clone-volume"}}},
                 f"/apis/longhorn.io/v1beta2/namespaces/{d.STORAGE_NS}/volumes/clone-volume":{"status":{"restoreInitiated":True,"restoreRequired":False}},
                 d.BASE+"/backup-test":backup}
        if legacy:objects[job]={"metadata":metadata,"spec":{"template":{"spec":{"containers":[{"securityContext":{"capabilities":{"add":["DAC_READ_SEARCH"]}}}]}}}}
        class Kube:
            calls=[]
            def call(self,path,method="GET",value=None):
                self.calls.append((path,method,copy.deepcopy(value)))
                if method=="GET":
                    if path not in objects:raise urllib.error.HTTPError(path,404,"missing",None,None)
                    return copy.deepcopy(objects[path])
                return value
        return backup,Kube(),job

    def test_clone_verifier_satisfies_baseline_security_and_cannot_write_data(self):
        backup,kube,_=self.setup_fixture()
        original=d.IMAGE;d.IMAGE="example/provider@sha256:"+"a"*64
        try:d.step(kube,backup)
        finally:d.IMAGE=original
        created=next(v for _,method,v in kube.calls if method=="POST")
        spec=created["spec"]["template"]["spec"];container=spec["containers"][0]
        self.assertFalse(spec["automountServiceAccountToken"])
        self.assertTrue(container["volumeMounts"][0]["readOnly"])
        self.assertTrue(spec["volumes"][0]["persistentVolumeClaim"]["readOnly"])
        self.assertTrue(container["securityContext"]["readOnlyRootFilesystem"])
        self.assertFalse(container["securityContext"]["allowPrivilegeEscalation"])
        self.assertEqual(container["securityContext"]["capabilities"],{"drop":["ALL"],"add":["DAC_OVERRIDE"]})
        self.assertNotIn("flash.heterocloud.io/instance",created["spec"]["template"]["metadata"]["labels"])
        self.assertEqual(container["command"][-1],d.VERIFY)

    def test_only_an_owned_rejected_job_is_replaced(self):
        backup,kube,job=self.setup_fixture(legacy=True)
        d.step(kube,backup)
        deletes=[(path,v) for path,method,v in kube.calls if method=="DELETE"]
        self.assertEqual(deletes,[(job,{"apiVersion":"v1","kind":"DeleteOptions","propagationPolicy":"Foreground","preconditions":{"uid":"job-uid"}})])
        self.assertFalse(any(method=="POST" for _,method,_ in kube.calls))

    def test_timeout_retains_volume_identities_until_cleanup_is_verified(self):
        backup,kube,_=self.setup_fixture()
        state=json.loads(backup["metadata"]["annotations"][d.RESULT]);state["started_at"]=(dt.datetime.now(dt.timezone.utc)-dt.timedelta(hours=1)).isoformat()
        backup["metadata"]["annotations"][d.RESULT]=json.dumps(state)
        d.step(kube,backup)
        update=next(v for _,method,v in kube.calls if method=="PUT")
        result=json.loads(update["metadata"]["annotations"][d.RESULT])
        self.assertEqual(result["volume_name"],"clone-pv")
        self.assertEqual(result["volume_handle"],"clone-volume")
        self.assertEqual(result["phase"],"cleaning")
        self.assertNotIn("cleanup_verified",result)


if __name__=="__main__":unittest.main()
