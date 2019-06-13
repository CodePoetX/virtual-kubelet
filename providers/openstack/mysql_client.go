package openstack

import (
	"database/sql"
	"fmt"
	_ "github.com/go-sql-driver/mysql"
	"log"
)

var db *sql.DB

func init() {
	db, _ = sql.Open("mysql", "root:fudan_Nisl2019@@tcp(localhost:3306)/virtual_kubelet?charset=utf8")
	db.SetMaxOpenConns(100)
	db.SetMaxIdleConns(50)
	db.Ping()
}

func connectDB() {
	var err error
	db, err = sql.Open("mysql", "root:fudan_Nisl2019@@tcp(localhost:3306)/virtual_kubelet?charset=utf8")
	if err != nil {
		log.Fatal(err)
	}
	db.SetMaxOpenConns(100)
	db.SetMaxIdleConns(50)
	db.Ping()
}

//create pod and save pod key info to mysql db
func PodCreate(pod *ZunPod) (err error) {
	if db == nil {
		connectDB()
	}
	stmt, err := db.Prepare(`INSERT INTO Pod (NameSpace,Name,Namespace_Name,container_id,pod_kind,pod_uid,pod_createtime,pod_clustername,node_name) VALUES (?, ? , ?, ?, ?, ?, ?, ?, ?)`)
	defer func() {
		stmt.Close()
	}()
	checkErr(err)
	_, err = stmt.Exec(pod.NameSpace,
		pod.Name,
		pod.NamespaceName,
		pod.containerId,
		pod.podKind,
		pod.podUid,
		pod.podCreatetime,
		pod.podClustername,
		pod.nodeName)
	checkErr(err)
	return nil
}

//delete pod by namespaces-name
func PodDelete(nsn string) {
	if db == nil {
		connectDB()
	}
	stmt, _ := db.Prepare(`DELETE FROM Pod WHERE Namespace_Name=?`)
	defer func() {
		stmt.Close()
	}()
	res, _ := stmt.Exec(nsn)
	_, _ = res.RowsAffected()
}

//query pod info by namespaces-name
func PodQuery(nsn string) *ZunPod {
	pod := new(ZunPod)
	if db == nil {
		connectDB()
	}
	rows, _ := db.Query(`SELECT NameSpace,Name,Namespace_Name,container_id,pod_kind,pod_uid,pod_createtime,pod_clustername,node_name FROM Pod WHERE Namespace_Name=?`, nsn)
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
	if db == nil {
		connectDB()
	}
	rows, _ := db.Query(`SELECT container_id FROM Pod WHERE Namespace_Name=?`, nsn)
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
	if db == nil {
		connectDB()
	}
	rows, _ := db.Query(`SELECT NameSpace,Name,Namespace_Name,container_id,pod_kind,pod_uid,pod_createtime,pod_clustername,node_name FROM Pod WHERE container_id=?`, containerid)
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

func checkErr(err error) {
	if err != nil {
		fmt.Println(err)
		panic(err)
	}
}
