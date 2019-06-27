package openstack

import (
	"database/sql"
	"fmt"
	_ "github.com/go-sql-driver/mysql"
	"log"
)

func getMysqlClient() *sql.DB {
	mysqlClient, err := sql.Open("mysql", "root:fudan_Nisl2019@@tcp(10.10.87.62:3306)/virtual_kubelet?charset=utf8")
	if err != nil {
		log.Fatal(err)
	}
	err = mysqlClient.Ping()
	if err != nil {
		log.Fatal(err)
	}
	return mysqlClient
}

//create pod and save pod key info to mysql db
func PodCreate(pod *ZunPod) (err error) {
	db := getMysqlClient()
	defer func() {
		db.Close()
	}()
	_, err = db.Exec(
		"INSERT INTO Pod (NameSpace,Name,Namespace_Name,container_id,pod_kind,pod_uid,pod_createtime,pod_clustername,node_name) VALUES (?, ? , ?, ?, ?, ?, ?, ?, ?)",
		pod.NameSpace,
		pod.Name,
		pod.NamespaceName,
		pod.containerId,
		pod.podKind,
		pod.podUid,
		pod.podCreatetime,
		pod.podClustername,
		pod.nodeName,
	)
	if err != nil {
		return fmt.Errorf("create pod is error : %s ", err)
	}
	return nil
}

//delete pod by namespaces-name
func PodDelete(nsn string) {
	db := getMysqlClient()
	stmt, _ := db.Prepare(`DELETE FROM Pod WHERE Namespace_Name=?`)
	defer func() {
		stmt.Close()
		db.Close()
	}()
	res, _ := stmt.Exec(nsn)
	_, _ = res.RowsAffected()
}

//query pod info by namespaces-name
func PodQuery(nsn string) *ZunPod {
	pod := new(ZunPod)
	db := getMysqlClient()
	rows, _ := db.Query(`SELECT NameSpace,Name,Namespace_Name,container_id,pod_kind,pod_uid,pod_createtime,pod_clustername,node_name FROM Pod WHERE Namespace_Name=?`, nsn)
	defer func() {
		rows.Close()
		db.Close()
	}()
	for rows.Next() {
		var NameSpace string
		var Name string
		var NamespaceName string
		var containerId string
		var podKind string
		var podUid string
		var podCreatetime string
		var podClustername string
		var nodeName string
		if err := rows.Scan(&NameSpace, &Name, &NamespaceName, &containerId, &podKind, &podUid, &podCreatetime, &podClustername, &nodeName); err != nil {
			log.Fatal(err)
		}
		pod.NameSpace = NameSpace
		pod.Name = Name
		pod.NamespaceName = NamespaceName
		pod.containerId = containerId
		pod.podKind = podKind
		pod.podUid = podUid
		pod.podCreatetime = podCreatetime
		pod.podClustername = podClustername
		pod.nodeName = nodeName
	}
	return pod
}

// query container id by namespaces-name
func ContainerIdQuery(nsn string) (containerid string) {
	db := getMysqlClient()
	rows, _ := db.Query(`SELECT container_id FROM Pod WHERE Namespace_Name=?`, nsn)
	defer func() {
		rows.Close()
		db.Close()
	}()
	for rows.Next() {
		var containerId string
		if err := rows.Scan(&containerId); err != nil {
			log.Fatal(err)
		}
		containerid = containerId
	}
	return
}

//query pod info by container id
func PodQueryByContainerId(containerid string) *ZunPod {
	db := getMysqlClient()
	rows, _ := db.Query(`SELECT NameSpace,Name,Namespace_Name,container_id,pod_kind,pod_uid,pod_createtime,pod_clustername,node_name FROM Pod WHERE container_id=?`, containerid)
	defer func() {
		rows.Close()
		db.Close()
	}()
	for rows.Next() {
		pod := new(ZunPod)
		var NameSpace string
		var Name string
		var NamespaceName string
		var containerId string
		var podKind string
		var podUid string
		var podCreatetime string
		var podClustername string
		var nodeName string
		if err := rows.Scan(&NameSpace, &Name, &NamespaceName, &containerId, &podKind, &podUid, &podCreatetime, &podClustername, &nodeName); err != nil {
			log.Fatal(err)
		}
		pod.NameSpace = NameSpace
		pod.Name = Name
		pod.NamespaceName = NamespaceName
		pod.containerId = containerId
		pod.podKind = podKind
		pod.podUid = podUid
		pod.podCreatetime = podCreatetime
		pod.podClustername = podClustername
		pod.nodeName = nodeName
		return pod
	}
	return nil
}
