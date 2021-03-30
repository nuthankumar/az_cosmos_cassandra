package utils

import (
	"regexp"
	"strings"

	sample "github.com/nuthankumar/cosmosdb/pkg/apis/dbprovision/v1"
)

//const debug = false
func ValidateClientSession(client_cosmosCassandraContactPoint, cosmosCassandraContactPoint, client_CosmosCassandraPassword, cosmosCassandraPassword string) bool {
	LogInfo("Validating Client Contact point and password from CRD request YAML.")
	if client_cosmosCassandraContactPoint != cosmosCassandraContactPoint || client_CosmosCassandraPassword != cosmosCassandraPassword {
		LogError("Contact points and cosmos password does not match")
		return false
	}
	return true
}

//validating update cosmos crd
func ValidateUpdateCrd(clientID, clientNamespace, roleName, newClientID, newClientNamespace, newRolename string) bool {
	LogInfo("Validating Update CRD request YAML.")
	if clientID != newClientID || clientNamespace != newClientNamespace || roleName != newRolename {
		LogError("CRD Yaml files does not match. Failed to update provisioning")
		return false
	}
	return true
}

//validating update cosmos crd
func CheckForChanges(keyspacename, newKeyspacename string) ([]string, []string) {
	LogInfo("Checking for changes in keyspace field")
	var keyspace = StripSpaces(keyspacename)
	var newKeyspace = StripSpaces(newKeyspacename)
	if keyspace != newKeyspace {
		LogInfo("Changes detected")
		var deleteKeyspaces, createKeyspaces []string
		var keyspaces []string = strings.Split(keyspace, ",")
		var newKeyspaces []string = strings.Split(newKeyspace, ",")
		for _, item := range keyspaces {
			if !contains(newKeyspaces, item) {
				deleteKeyspaces = append(deleteKeyspaces, item)
			}
		}
		for _, item := range newKeyspaces {
			if !contains(keyspaces, item) {
				createKeyspaces = append(createKeyspaces, item)
			}
		}
		return createKeyspaces, deleteKeyspaces
	} else {
		LogInfo("No changes detected")
		return nil, nil
	}
}
func ValidateCrd(data *sample.DBProvisioning) bool {
	LogInfo("Validating CRD request YAML")
	if data.Spec.Datacenter != "ccs" {
		LogError("Invalid Datacenter name, it should be ccs .")
		return false
	}

	if data.Spec.KeyspaceTopology != "NetworkTopologyStrategy" {
		LogError("Invalid KeyspaceTopology name, it should be NetworkTopologyStrategy")
		return false
	}
	isNumeric := regexp.MustCompile(`^[1-3]+$`).MatchString
	var replicationfactors = strings.Split(StripSpaces(data.Spec.ReplicationFactor), ",")
	if len(replicationfactors) > 1 {
		LogError("Cannot have multiple replicationfactors")
	} else {
		for _, replicationfactor := range replicationfactors {
			if !isNumeric(replicationfactor) {
				LogError("Invalid replicationfactor, Only numeric between 1-3 is allowed, given value=" + replicationfactor)
				return false
			}
		}
	}
	//isAlphaNumericWithHyphen := regexp.MustCompile(`^[a-zA-Z0-9-]+$`).MatchString
	isAlphaNumericWithoutHyphen := regexp.MustCompile(`^[a-zA-Z0-9]+$`).MatchString
	var keyspaces = strings.Split(StripSpaces(data.Spec.Keyspace), ",")
	for _, keyspace := range keyspaces {
		if !isAlphaNumericWithoutHyphen(keyspace) {
			LogError("Invalid Keyspace name, Only alpha numeric is allowed, given value=" + keyspace)
			return false
		}

	}

	//var stringvalueclientid string = StripSpaces(clientid)
	var clientids = strings.Split(StripSpaces(data.Spec.ClientID), ",")
	if len(clientids) > 1 {
		LogError("Cannot have multiple clientids")
	} else {
		for _, clientid := range clientids {
			if !isAlphaNumericWithoutHyphen(clientid) {
				LogError("Invalid clientid name, Only alpha numeric is allowed, given value=" + clientid)
				return false
			}
		}
	}
	var rolenames = strings.Split(StripSpaces(data.Spec.Rolename), ",")
	if len(rolenames) > 1 {
		LogError("Cannot have multiple Roles for on CRD")
	} else {
		for _, rolename := range rolenames {
			if !isAlphaNumericWithoutHyphen(rolename) {
				LogError("Invalid Role name Only alpha numeric is allowed, given value=" + rolename)
				return false
			}
		}
	}
	if data.Spec.CreateSchema {
		validated := validateCreateSchema(data.Spec.Configmap, data.Spec.SchemaScriptName, data.Spec.DefaultValueScriptName)
		if !validated {
			return false
		}
	}
	return true
}

func validateCreateSchema(configmap, schemaScriptName, defaultValueScriptName string) bool {
	if configmap == "" || schemaScriptName == "" || defaultValueScriptName == "" {
		LogError("Invalid schema creation request. Configmapname,schemascriptname and defaultvaluescriptname cannot be empty ")
		return false
	}
	return true
}
