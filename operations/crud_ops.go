package operations

import (
	"fmt"
	"math/rand"
	"strings"

	"github.com/gocql/gocql"
	utility "github.com/nuthankumar/az_cosmos_cassandra/utils"
	sample "github.com/nuthankumar/cosmosdb/pkg/apis/dbprovision/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/client-go/kubernetes"
)

const (
	debug      = false
	CrKeyspace = "CREATE KEYSPACE %s WITH REPLICATION = { 'class' : 'NetworkTopologyStrategy', 'datacenter1' : 1 }"
	ckeyspace  = "CREATE KEYSPACE  %s WITH replication = {'class': '%s', '%s': '%s'}  AND durable_writes = true"
)

func SearchKeyspaces(session *gocql.Session, keyspaces string) bool {
	utility.LogInfo("Check If Keyspaces Exists method called for keyspaces " + keyspaces)
	if session == nil {
		utility.LogInfo("Not Able to create session to check keyspaces")
		return false
	}
	var keyspace_name string
	keyspacesqry := session.Query("SELECT keyspace_name FROM system_schema.keyspaces;").Iter()
	//sleep_time_output.Scan(&sleep_time_hours)
	var keyspacesarr []string = strings.Split(utility.StripSpaces(keyspaces), ",")
	for _, keyspace := range keyspacesarr {
		for keyspacesqry.Scan(&keyspace_name) {
			if utility.StripSpaces(keyspace_name) == keyspace {
				utility.LogError("keyspace found --> " + keyspace)

				return false
			}
		}
	}
	if err := keyspacesqry.Close(); err != nil {

		return false
	}

	return true
}

func SearchConfigMap(createschema bool, clientNamespace, configmap, schemaScriptName, defaultValueScriptName string, client *kubernetes.Clientset) (map[string]string, bool) {
	if !createschema {
		return nil, true
	}
	configMap, err := client.CoreV1().ConfigMaps(clientNamespace).Get(configmap, metav1.GetOptions{})
	if err != nil {
		utility.LogError("Error finding configmap: " + configmap)
		return nil, false
	}
	var schemaScriptflag bool = false
	var defaultValueScriptflag bool = false
	for key, _ := range configMap.Data {
		if key == schemaScriptName {
			schemaScriptflag = true
		}
		if key == defaultValueScriptName {
			defaultValueScriptflag = true
		}

	}
	if schemaScriptflag && defaultValueScriptflag {
		return configMap.Data, true
	}
	utility.LogError(schemaScriptName + " or " + defaultValueScriptName + " doesnot exist in configmap " + configmap)
	return nil, false
}

func CreateAllOption(session *gocql.Session, data *sample.DBProvisioning, configmapData map[string]string, client *kubernetes.Clientset) (string, bool) {

	output1, err1 := createKeyspaces(session, data.Spec.Keyspace, data.Spec.KeyspaceTopology, data.Spec.Datacenter, data.Spec.Rolename, data.Spec.ReplicationFactor)
	if !err1 {
		fmt.Println("STDERR:", output1)
		return output1, false
	}
	if data.Spec.CreateSchema {
		output3, err3 := runSchemaScriptOnK8s(session, client, data.Spec.SchemaScriptName, configmapData)
		if !output3 {
			utility.LogError(err3.Error())
			return err3.Error(), output3
		}
		output4, err4 := runSchemaScriptOnK8s(session, client, data.Spec.DefaultValueScriptName, configmapData)
		if !output4 {
			utility.LogError(err4.Error())
			return err4.Error(), output4
		}
	}
	return "CreateAllOption", true
}
func createKeyspaces(session *gocql.Session, keyspace, keyspaceTopology, datacenter, rolename, replicationfactor string) (string, bool) {

	var keyspaces []string = strings.Split(utility.StripSpaces(keyspace), ",")
	for _, keyspace := range keyspaces {
		fmt.Println(keyspace)
		output, err := CreateKeyspace(session, keyspace, keyspaceTopology, datacenter, replicationfactor)
		if !err {
			fmt.Println("STDERR:", output)
			return output, false
		}
	}
	return "Keyspaces created successfully", true
}

func CreateKeyspace(session *gocql.Session, keyspacename, keyspaceTopology, datacenter, replicationfactor string) (string, bool) {
	utility.LogInfo("creating Keyspace:" + keyspacename)
	if session == nil {
		utility.LogInfo("Not Able to create session to create Keyspace")
		return "CreateKeyspace", false
	}
	// create Keyspace
	err := utility.ExecuteQuery(fmt.Sprintf(ckeyspace, keyspacename, keyspaceTopology, datacenter, replicationfactor), session)
	//err := session.Query("CREATE KEYSPACE " + keyspacename + " WITH replication = {'class': '" + keyspaceTopology + "', '" + datacenter + "': '" + replicationfactor + "'}  AND durable_writes = true").Exec()
	if err != nil {
		utility.LogError("Error creating keyspace " + keyspacename + " with " + err.Error())
		fmt.Errorf("failed to create keyspace with error %+v", err)

		return "CreateKeyspace", false
	}

	return "CreateKeyspace", true
}
func runSchemaScriptOnK8s(session *gocql.Session, client *kubernetes.Clientset, scriptname string, configmapData map[string]string) (bool, error) {
	if session == nil {
		utility.LogInfo("Not Able to create session to run Schema Scripts")
		return false, nil
	}
	for key, value := range configmapData {
		if key == scriptname {
			commands := strings.Split(strings.TrimSpace(value), ";")
			for i := range commands {
				if i == len(commands)-1 {
					utility.LogInfo("running completed:" + scriptname)

					return true, nil
				}
				err := session.Query(commands[i]).Exec()
				if err != nil {
					utility.LogError("Error while executing " + commands[i] + " with " + err.Error())
					fmt.Errorf("Error %+v", err)

					return false, err
				}
			}

		}
	}

	return true, nil
}

// func Rollback(session *gocql.Session, rolename, keyspace, clientNamespace, outputSecret, usr, clientID string, client *kubernetes.Clientset) bool {
// 	fmt.Println("RollbackAll keyspaces ")
// 	if session == nil {
// 		utility.LogInfo("Not Able to create session to roleback")
// 		return false
// 	}
// 	var keyspaces []string = strings.Split(utility.StripSpaces(keyspace), ",")
// 	for _, keyspace := range keyspaces {
// 		// create Keyspace
// 		err := session.Query("DROP KEYSPACE IF EXISTS " + keyspace).Exec()
// 		if err != nil {
// 			if strings.Contains(err.Error(), "timeout") {
// 				for i := 0; i < 3; i++ {
// 					errnew := session.Query("DROP KEYSPACE IF EXISTS " + keyspace).Exec()
// 					if errnew != nil {
// 						utility.LogInfo("Try to Delete keyspace again" + errnew.Error())
// 						if !strings.Contains(errnew.Error(), "timeout") {
// 							utility.LogError("Error dropping keyspace " + keyspace + " with " + errnew.Error())
// 							utility.LogError("Cleanup failed, Failed to drop keyspace")
//
// 							return false
// 						} else {
// 							continue
// 						}
// 					}
// 					break

// 				}
// 			} else {
// 				utility.LogError("Error dropping keyspace " + keyspace + " with " + err.Error())
// 				utility.LogError("Cleanup failed, Failed to drop keyspace")
//
// 				return false
// 			}
// 		}
// 	}
// 	fmt.Println("RollbackAll rolename ", rolename+usr)

// 	err := session.Query("DROP ROLE IF EXISTS " + rolename + usr).Exec()
// 	if err != nil {
// 		utility.LogError("Error dropping Role " + rolename + usr + " with error " + err.Error())
// 		utility.LogError("Cleanup failed, Failed to drop roles")
//
// 		return false
// 	}
//
// 	// if outputSecret != (clientID + "-" + usr) {
// 	// 	outputsecusr, _ := DeleteSecret(clientID+"-"+usr, clientNamespace, client)
// 	// 	fmt.Println("output RollbackAll Secret usr ", outputsecusr)
// 	// }

// 	return true
// }

func RandomString(len int) string {
	bytes := make([]byte, len)
	for i := 0; i < len; i++ {
		bytes[i] = byte(65 + rand.Intn(25)) //A=65 and Z = 65+25
	}
	return string(bytes)
}
