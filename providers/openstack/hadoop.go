package openstack

import (
	"database/sql"
	"fmt"
	_ "github.com/go-sql-driver/mysql"
	zun_container "github.com/gophercloud/gophercloud/openstack/container/v1/container"
	"log"
	"strconv"
	"strings"
	"sync"
)

type hosts struct {
	name        string
	ip          string
	containerId string
}

type HadoopCluster struct {
	clusterName     string
	numberOfNodes   int
	availableNumber int
	createTime      string
	hadoopMasterId  string
	hadoopSlaveId   string
	clusterStatus   string
	namespace       string
}

type HadoopNode struct {
	namespaces  string
	name        string
	nodeType    string
	containerId string
	ip          string
	status      string
}

var mutex sync.Mutex

func getHadoopMysqlClient() *sql.DB {
	mysqlClient, err := sql.Open("mysql", "root:fudan_Nisl2019@@tcp(localhost:3306)/hadoop?charset=utf8")
	if err != nil {
		log.Fatal(err)
	}
	err = mysqlClient.Ping()
	if err != nil {
		log.Fatal(err)
	}
	return mysqlClient
}

func initHadoopCluster(cluster *HadoopCluster) error {
	mutex.Lock()
	db := getHadoopMysqlClient()
	defer func() {
		db.Close()
		mutex.Unlock()
	}()
	if isExitHadoopCluster(cluster.namespace, cluster.clusterName) {
		return nil
	}
	_, err := db.Exec(
		"INSERT INTO hadoop_cluster (cluster_name,number_of_nodes,available_number,create_time,hadoop_master_id,hadoop_slave_id,cluster_status,namespace) VALUES (?, ?, ? , ?, ?, ?, ?, ?)",
		cluster.clusterName,
		cluster.numberOfNodes,
		cluster.availableNumber,
		cluster.createTime,
		cluster.hadoopMasterId,
		cluster.hadoopSlaveId,
		cluster.clusterStatus,
		cluster.namespace,
	)
	if err != nil {
		fmt.Printf("\n hadoop cluster init to mysql db is failed.reason is :%s \n", err)
		return fmt.Errorf("\n hadoop cluster init to mysql db is failed.reason is :%s \n", err)
	}
	return nil
}

//delete pod by namespaces-name
func deleteHadoopCluster(namespaces, cName string) {
	deleteHadoopNodes(namespaces, cName)
	db := getHadoopMysqlClient()
	stmt, _ := db.Prepare(`DELETE FROM hadoop_cluster WHERE namespace=? AND cluster_name=?`)
	defer func() {
		stmt.Close()
		db.Close()
	}()
	if !isExitHadoopCluster(namespaces, cName) {
		return
	}
	res, _ := stmt.Exec(namespaces, cName)
	_, _ = res.RowsAffected()
}

func getHadoopMasterNodeId(namespaces, cName string) (masterId string) {
	db := getHadoopMysqlClient()
	rows, _ := db.Query(`SELECT hadoop_master_id FROM hadoop_cluster WHERE namespace=? AND cluster_name=?`, namespaces, cName)
	defer func() {
		rows.Close()
		db.Close()
	}()
	for rows.Next() {
		if err := rows.Scan(&masterId); err != nil {
			log.Fatal(err)
		}
	}
	return
}

func getHadoopSlaveNodeId(namespaces, cName string) (slaveIds []string) {
	db := getHadoopMysqlClient()
	rows, _ := db.Query(`SELECT hadoop_slave_id FROM hadoop_cluster WHERE namespace=? AND cluster_name=?`, namespaces, cName)
	defer func() {
		rows.Close()
		db.Close()
	}()
	var slaves string
	for rows.Next() {
		if err := rows.Scan(&slaves); err != nil {
			log.Fatal(err)
		}
	}
	if slaves != "" {
		slaveIds = strings.Split(slaves, "@-@")
	}
	return
}

func deleteHadoopNodes(namespaces, cName string) {
	if masterId := getHadoopMasterNodeId(namespaces, cName); masterId != "" {
		deleteHadoopNodeByContainerId(masterId)
	}
	if slaveIds := getHadoopSlaveNodeId(namespaces, cName); len(slaveIds) > 0 {
		for _, v := range slaveIds {
			deleteHadoopNodeByContainerId(v)
		}
	}
}

func deleteHadoopNodeByContainerId(containerId string) {
	db := getHadoopMysqlClient()
	stmt, _ := db.Prepare(`DELETE FROM hadoop_nodes WHERE container_id=?`)
	defer func() {
		stmt.Close()
		db.Close()
	}()
	res, _ := stmt.Exec(containerId)
	_, _ = res.RowsAffected()
}

func getNumberOfNodesbyNnameCname(namespaces, cName string) (numberOfNodes int) {
	db := getHadoopMysqlClient()
	rows, _ := db.Query(`SELECT number_of_nodes FROM hadoop_cluster WHERE namespace=? AND cluster_name=?`, namespaces, cName)
	defer func() {
		rows.Close()
		db.Close()
	}()
	for rows.Next() {
		if err := rows.Scan(&numberOfNodes); err != nil {
			log.Fatal(err)
		}
	}
	return
}

func isExitHadoopCluster(namespaces, cName string) (flag bool) {
	db := getHadoopMysqlClient()
	rows, _ := db.Query(`SELECT number_of_nodes FROM hadoop_cluster WHERE namespace=? AND cluster_name=?`, namespaces, cName)
	defer func() {
		rows.Close()
		db.Close()
	}()
	return rows.Next()
}

func (p *ZunProvider) ContainerHadoopNodeFactory(c *zun_container.Container, namespace, name string) {
	mutex.Lock()
	defer mutex.Unlock()
	if c.Status == "Created" {
		tempName := name
		clusterName := tempName[0:strings.LastIndex(tempName[0:strings.LastIndex(tempName, "-")], "-")]
		if masterId := getHadoopMasterNodeId(namespace, clusterName); masterId == "" {
			hadoopNode := &HadoopNode{
				namespaces:  namespace,
				name:        name,
				nodeType:    "hadoop-master",
				containerId: c.UUID,
				ip:          c.GetIp(),
				status:      "Created",
			}
			addHadoopNode(hadoopNode)
			updateHadoopClusterMasterId(c.UUID, namespace, clusterName)
		} else {
			hadoopNode := &HadoopNode{
				namespaces:  namespace,
				name:        name,
				nodeType:    "hadoop-slave",
				containerId: c.UUID,
				ip:          c.GetIp(),
				status:      "Created",
			}
			addHadoopNode(hadoopNode)
			updateHadoopClusterSlaveIds(c.UUID, namespace, clusterName)
		}

	}

	if c.Status == "Running" {
		if isRunningHadoopNode(c.UUID) {
			return
		} else {
			tempName := name
			clusterName := tempName[0:strings.LastIndex(tempName[0:strings.LastIndex(tempName, "-")], "-")]
			updateHadoopNodeStatusToRunning(c.UUID)
			updatehadoopClusterAvailableNumber(namespace, clusterName)
			if getNumberOfNodesbyNnameCname(namespace, clusterName) == getHadoopClusterAvailableNumber(namespace, clusterName) {
				updatehadoopClusterCreateStatus(namespace, clusterName, "Creating")
				p.ConfigAndStartContainerHadoopCluster(namespace, clusterName)
			}
		}
	}
}

func (p *ZunProvider) ConfigAndStartContainerHadoopCluster(namespace, cName string) {
	hadoopMasterId := getHadoopMasterNodeId(namespace, cName)
	hadoopSlaveIdList := getHadoopSlaveNodeId(namespace, cName)
	HostsStr, slavesStr := getFinallyHostsConfigAndSlavesConfig(namespace, cName)
	p.WriteHadoopConfigFile(hadoopMasterId, HostsStr, slavesStr)
	for _, v := range hadoopSlaveIdList {
		p.WriteHadoopConfigFile(v, HostsStr, slavesStr)
	}

	p.StartContainerHadoopCluster(hadoopMasterId)
	fmt.Println("Hadoop Cluster created is success")
	updatehadoopClusterCreateStatus(namespace, cName, "Running")
}

func (p *ZunProvider) StartContainerHadoopCluster(containerId string) {
	formatHdfsStr := fmt.Sprintf("hadoop namenode -format")
	formatHdfsCommand := zun_container.ExcuteOpts{
		//Command:"cat << EOF >   /etc/hosts  \n [global] \n EOF",
		Command:     formatHdfsStr,
		Run:         false,
		Interacitve: false,
	}
	_, err := zun_container.Exexcute(p.ZunClient, containerId, formatHdfsCommand).Extract()
	if err != nil {
		fmt.Printf("executeErr is %v\n", err)
	}

	startHadoopClusterStr := fmt.Sprintf("/usr/local/hadoop/sbin/start-all.sh")
	startHadoopClusterCommand := zun_container.ExcuteOpts{
		//Command:"cat << EOF >   /etc/hosts  \n [global] \n EOF",
		Command:     startHadoopClusterStr,
		Run:         false,
		Interacitve: false,
	}
	_, _ = zun_container.Exexcute(p.ZunClient, containerId, startHadoopClusterCommand).Extract()
	//if err != nil {
	//	fmt.Printf("executeErr is %v\n", err)
	//}
}

func getFinallyHostsConfigAndSlavesConfig(namespace, clusterName string) (hostsStr, slaveStr string) {
	hadoopMasterId := getHadoopMasterNodeId(namespace, clusterName)
	masterHost := getHadoopHosts(hadoopMasterId)
	hostsStr = fmt.Sprintf("%s	%s\\n", masterHost.ip, "hadoop-master")

	hadoopSlaveIdList := getHadoopSlaveNodeId(namespace, clusterName)

	for i := 0; i < len(hadoopSlaveIdList); i++ {
		slaveHost := getHadoopHosts(hadoopSlaveIdList[i])
		slaveName := fmt.Sprintf("hadoop-slave%s", strconv.Itoa(i+1))
		hostsStr += fmt.Sprintf("%s	%s\\n", slaveHost.ip, slaveName)
		slaveStr += fmt.Sprintf("%s\\n", slaveName)
	}
	return
}

func (p *ZunProvider) WriteHadoopConfigFile(containerId, HostsStr, slavesStr string) {
	cpStr := fmt.Sprintf("cp /etc/hosts /etc/hosts.temp")
	cpCommand := zun_container.ExcuteOpts{
		//Command:"cat << EOF >   /etc/hosts  \n [global] \n EOF",
		Command:     cpStr,
		Run:         false,
		Interacitve: false,
	}
	_, err := zun_container.Exexcute(p.ZunClient, containerId, cpCommand).Extract()
	if err != nil {
		fmt.Printf("executeErr is %v\n", err)
	}

	wHostsStr := fmt.Sprintf("sed -i '$ a %s' /etc/hosts.temp", HostsStr)
	wHostsCommand := zun_container.ExcuteOpts{
		//Command:"cat << EOF >   /etc/hosts  \n [global] \n EOF",
		Command:     wHostsStr,
		Run:         false,
		Interacitve: false,
	}
	_, err = zun_container.Exexcute(p.ZunClient, containerId, wHostsCommand).Extract()
	if err != nil {
		fmt.Printf("executeErr is %v\n", err)
	}

	finCpStr := fmt.Sprintf("cp /etc/hosts.temp /etc/hosts")
	finCpCommand := zun_container.ExcuteOpts{
		//Command:"cat << EOF >   /etc/hosts  \n [global] \n EOF",
		Command:     finCpStr,
		Run:         false,
		Interacitve: false,
	}
	_, err = zun_container.Exexcute(p.ZunClient, containerId, finCpCommand).Extract()
	if err != nil {
		fmt.Printf("executeErr is %v\n", err)
	}

	wSlavesStr := fmt.Sprintf("sed -i '$ a %s' /usr/local/hadoop/etc/hadoop/slaves", slavesStr)
	wSlavesCommand := zun_container.ExcuteOpts{
		//Command:"cat << EOF >   /etc/hosts  \n [global] \n EOF",
		Command:     wSlavesStr,
		Run:         false,
		Interacitve: false,
	}
	_, err = zun_container.Exexcute(p.ZunClient, containerId, wSlavesCommand).Extract()
	if err != nil {
		fmt.Printf("executeErr is %v\n", err)
	}
	dSlaves1dStr := fmt.Sprintf("sed -i '1d' /usr/local/hadoop/etc/hadoop/slaves")
	dSlaves1dCommand := zun_container.ExcuteOpts{
		//Command:"cat << EOF >   /etc/hosts  \n [global] \n EOF",
		Command:     dSlaves1dStr,
		Run:         false,
		Interacitve: false,
	}
	_, err = zun_container.Exexcute(p.ZunClient, containerId, dSlaves1dCommand).Extract()
	if err != nil {
		fmt.Printf("executeErr is %v\n", err)
	}
}

func addHadoopNode(node *HadoopNode) error {
	db := getHadoopMysqlClient()
	defer func() {
		db.Close()
	}()
	if isExitHadoopNode(node.containerId) {
		return nil
	}
	_, err := db.Exec(
		"INSERT INTO hadoop_nodes (namespaces,name,node_type,container_id,ip,status) VALUES (?, ?, ? , ?, ?, ?)",
		node.namespaces,
		node.name,
		node.nodeType,
		node.containerId,
		node.ip,
		node.status,
	)
	if err != nil {
		fmt.Printf("\n hadoop node add to mysql db is failed.reason is :%s \n", err)
		return fmt.Errorf("\n hadoop node add to mysql db is failed.reason is :%s \n", err)
	}
	return nil
}

func isExitHadoopNode(containerId string) (flag bool) {
	db := getHadoopMysqlClient()
	rows, _ := db.Query(`SELECT container_id FROM hadoop_nodes WHERE container_id=?`, containerId)
	defer func() {
		rows.Close()
		db.Close()
	}()
	return rows.Next()
}

func isRunningHadoopNode(containerId string) (flag bool) {
	db := getHadoopMysqlClient()
	rows, _ := db.Query(`SELECT status FROM hadoop_nodes WHERE container_id=?`, containerId)
	defer func() {
		rows.Close()
		db.Close()
	}()
	var status string
	for rows.Next() {
		if err := rows.Scan(&status); err != nil {
			log.Fatal(err)
		}
	}
	if status == "Running" {
		return true
	}
	return false
}

func updateHadoopNodeStatusToRunning(containerId string) {
	db := getHadoopMysqlClient()
	stmt, err := db.Prepare("UPDATE hadoop_nodes SET status = ? WHERE container_id=?")
	if err != nil {
		log.Fatal(err)
	}
	_, err = stmt.Exec("Running", containerId)

}

func updatehadoopClusterAvailableNumber(namespaces, name string) {
	avaNumber := getHadoopClusterAvailableNumber(namespaces, name)
	avaNumber += 1
	db := getHadoopMysqlClient()
	stmt, err := db.Prepare("UPDATE hadoop_cluster SET available_number = ? WHERE namespace=? AND cluster_name=?")
	if err != nil {
		log.Fatal(err)
	}
	_, err = stmt.Exec(avaNumber, namespaces, name)
}

func getHadoopClusterAvailableNumber(namespaces, cName string) (avaNumber int) {
	db := getHadoopMysqlClient()
	rows, _ := db.Query(`SELECT available_number FROM hadoop_cluster WHERE namespace=? AND cluster_name=?`, namespaces, cName)
	defer func() {
		rows.Close()
		db.Close()
	}()
	for rows.Next() {
		if err := rows.Scan(&avaNumber); err != nil {
			log.Fatal(err)
		}
	}
	return
}

func updateHadoopClusterMasterId(masterId, namespaces, name string) {
	db := getHadoopMysqlClient()
	stmt, err := db.Prepare("UPDATE hadoop_cluster SET hadoop_master_id = ? WHERE namespace=? AND cluster_name=?")
	if err != nil {
		log.Fatal(err)
	}
	_, err = stmt.Exec(masterId, namespaces, name)
}

func updateHadoopClusterSlaveIds(slaveIds, namespaces, name string) {
	slaveList := getHadoopSlaveNodeId(namespaces, name)
	if len(slaveList) == 1 {
		for _, v := range slaveList {
			slaveIds = fmt.Sprintf("%s@-@%s", slaveIds, v)
		}
	} else if len(slaveList) > 1 {
		for _, v := range slaveList {
			slaveIds = fmt.Sprintf("%s@-@%s", slaveIds, v)
		}
	}

	db := getHadoopMysqlClient()
	stmt, err := db.Prepare("UPDATE hadoop_cluster SET hadoop_slave_id = ? WHERE namespace=? AND cluster_name=?")
	if err != nil {
		log.Fatal(err)
	}
	_, err = stmt.Exec(slaveIds, namespaces, name)
}

func updatehadoopClusterCreateStatus(namespaces, name, status string) {
	db := getHadoopMysqlClient()
	stmt, err := db.Prepare("UPDATE hadoop_cluster SET cluster_status = ? WHERE namespace=? AND cluster_name=?")
	if err != nil {
		log.Fatal(err)
	}
	_, err = stmt.Exec(status, namespaces, name)
	//lastId, err := res.LastInsertId()
	//if err != nil {
	//	log.Fatal(err)
	//}
	//rowCnt, err := res.RowsAffected()
	//if err != nil {
	//	log.Fatal(err)
	//}
	//fmt.Printf("Update Hadoop cluster create status ID=%d, affected=%d\n", lastId, rowCnt)
}

func getHadoopHosts(containerId string) *hosts {
	db := getHadoopMysqlClient()
	rows, _ := db.Query(`SELECT ip,container_id FROM hadoop_nodes WHERE container_id=?`, containerId)
	defer func() {
		rows.Close()
		db.Close()
	}()
	var ip, cid string
	for rows.Next() {
		if err := rows.Scan(&ip, &cid); err != nil {
			log.Fatal(err)
			return nil
		}
	}
	h := &hosts{
		name:        "hadoop-node",
		ip:          ip,
		containerId: cid,
	}
	return h
}
