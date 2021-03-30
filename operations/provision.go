package operations

import (
	"github.com/gocql/gocql"
	utility "github.com/nuthankumar/az_cosmos_cassandra/utils"
	sample "github.com/nuthankumar/cosmosdb/pkg/apis/dbprovision/v1"
	"k8s.io/client-go/kubernetes"
)

func CreateCRD(data *sample.DBProvisioning, session *gocql.Session, clientset *kubernetes.Clientset) {
	keyspace_exists := SearchKeyspaces(session, data.Spec.Keyspace)
	cfgMapData, cfgMap_exists := SearchConfigMap(data.Spec.CreateSchema, data.Spec.ClientNamespace, data.Spec.Configmap, data.Spec.SchemaScriptName, data.Spec.DefaultValueScriptName, clientset)
	if !cfgMap_exists {
		utility.LogError("Provisioning Failed !Config map doesnot exist.")
		return
	}
	if keyspace_exists && cfgMap_exists && session != nil {
		utility.LogInfo("All requirements satisfied for Provisioning")
		output, err := CreateAllOption(session, data, cfgMapData, clientset)
		utility.LogInfo("create all output" + output)
		if !err {
			//Rollback(username,auth,service,data.Spec.Rolename, data.Spec.Keyspace, data.Spec.ClientNamespace, output, usr, data.Spec.ClientID,clientset)
		} else {
			utility.LogInfo("Provisioning successfull!")
		}
	}
}

// func UpdateCRD(oldData *sample.DBProvisioning, newData *sample.DBProvisioning, clientset *kubernetes.Clientset) {
// 	var service string = os.Getenv("SERVICE")
// 	username, auth := GetSecret(os.Getenv("SECRET_NAME"), os.Getenv("NAMESPACE"), clientset)
// 	if ScyllaStatus(service, username, auth) {

// 		validity := ValidateUpdateScyllaCrd(oldData.Spec.ClientID, oldData.Spec.ClientNamespace, oldData.Spec.Rolename, newData.Spec.ClientID, newData.Spec.ClientNamespace, newData.Spec.Rolename)
// 		if validity {
// 			session := GetSession(username, auth, service)
// 			if session == nil {
// 				utility.LogInfo("Not Able to create session to update and validate crd")
// 			}
// 			createKeyspaces, deleteKeyspaces := CheckForChanges(oldData.Spec.Keyspace, newData.Spec.Keyspace)
// 			for _, keyspace := range deleteKeyspaces {
// 				// create role
// 				err := session.Query("DROP KEYSPACE IF EXISTS " + keyspace).Exec()
// 				if err != nil {
// 					utility.LogError("Error dropping keyspace " + keyspace + " with " + err.Error())
// 					session.Close()
// 					if strings.Contains(err.Error(), "timeout") {
// 						for i := 0; i < 3; i++ {
// 							errnew := session.Query("DROP KEYSPACE IF EXISTS " + keyspace).Exec()
// 							if errnew != nil {
// 								utility.LogInfo("Try to Delete keyspace again" + errnew.Error())
// 								if !strings.Contains(errnew.Error(), "timeout") {
// 									utility.LogError("Error dropping keyspace " + keyspace + " with " + errnew.Error())
// 									utility.LogError("Cleanup failed, Failed to drop keyspace")
// 								} else {
// 									continue
// 								}
// 							}
// 							break

// 						}
// 					} else {
// 						utility.LogError("Error dropping keyspace " + keyspace + " with " + err.Error())
// 						utility.LogError("Cleanup failed, Failed to drop keyspace")
// 					}
// 				}
// 				utility.LogInfo("DELETEING KEYSPACE --> " + keyspace)
// 			}
// 			session.Close()
// 			for _, keyspace := range createKeyspaces {
// 				CreateKeyspace(username, auth, service, keyspace, newData.Spec.Datacenter, newData.Spec.KeyspaceTopology, newData.Spec.ReplicationFactor)
// 				GrantReadModifyAccess(username, auth, service, StripSpaces(newData.Spec.Rolename)+"user", keyspace)
// 			}
// 		}
// 	}
// }
