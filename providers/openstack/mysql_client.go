package openstack

import (
	"database/sql"
	"fmt"
	_ "github.com/go-sql-driver/mysql"
	"log"
	"os"
)
var dbvirtual_kubelet *sql.DB
func connectDBvirtualkubelet() {
	var err error
	username := os.Getenv("database_username")
	password := os.Getenv("database_password")
	database_ip := os.Getenv("database_ip")
	dbvirtual_kubelet, err = sql.Open("mysql",username+":"+password+"@tcp("+database_ip+ ":3306)/virtual_kubelet?charset=utf8")
	if err != nil {
		log.Fatal(err)
	}
	dbvirtual_kubelet.SetMaxOpenConns(100)
	dbvirtual_kubelet.SetMaxIdleConns(50)
	dbvirtual_kubelet.Ping()
}


//create pod and save pod key info to mysql db
func PodCreate(pod *ZunPod) (err error) {
	if dbvirtual_kubelet == nil {
		connectDBvirtualkubelet()
	}
	_, err = dbvirtual_kubelet.Exec(
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
	if dbvirtual_kubelet == nil {
		connectDBvirtualkubelet()
	}
	stmt, _ := dbvirtual_kubelet.Prepare(`DELETE FROM Pod WHERE Namespace_Name=?`)
	defer func() {
		stmt.Close()
		 
	}()
	res, _ := stmt.Exec(nsn)
	_, _ = res.RowsAffected()
}

//query pod info by namespaces-name
func PodQuery(nsn string) *ZunPod {
	pod := new(ZunPod)
	if dbvirtual_kubelet == nil {
		connectDBvirtualkubelet()
	}
	rows, _ := dbvirtual_kubelet.Query(`SELECT NameSpace,Name,Namespace_Name,container_id,pod_kind,pod_uid,pod_createtime,pod_clustername,node_name FROM Pod WHERE Namespace_Name=?`, nsn)
	defer func() {
		rows.Close()
		 
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
	if dbvirtual_kubelet == nil {
		connectDBvirtualkubelet()
	}
	rows, _ := dbvirtual_kubelet.Query(`SELECT container_id FROM Pod WHERE Namespace_Name=?`, nsn)
	defer func() {
		rows.Close()
		 
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
	if dbvirtual_kubelet == nil {
		connectDBvirtualkubelet()
	}
	rows, _ := dbvirtual_kubelet.Query(`SELECT NameSpace,Name,Namespace_Name,container_id,pod_kind,pod_uid,pod_createtime,pod_clustername,node_name FROM Pod WHERE container_id=?`, containerid)
	defer func() {
		rows.Close()
		 
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
