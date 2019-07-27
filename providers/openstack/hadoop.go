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

var dbHadoop *sql.DB

func connectDBHadoop() {
	var err error
	dbHadoop, err = sql.Open("mysql", "root:fudan_Nisl2019@@tcp(10.10.87.62:3306)/hadoop?charset=utf8")
	if err != nil {
		log.Fatal(err)
	}
	dbHadoop.SetMaxOpenConns(100)
	dbHadoop.SetMaxIdleConns(50)
	dbHadoop.Ping()
}


func initHadoopCluster(cluster *HadoopCluster) error {
	mutex.Lock()
	if dbHadoop == nil {
		connectDBHadoop()
	}
	defer func() {

		mutex.Unlock()
	}()
	if isExitHadoopCluster(cluster.namespace, cluster.clusterName) {
		return nil
	}
	_, err := dbHadoop.Exec(
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
	if dbHadoop == nil {
		connectDBHadoop()
	}
	stmt, _ := dbHadoop.Prepare(`DELETE FROM hadoop_cluster WHERE namespace=? AND cluster_name=?`)
	defer func() {
		stmt.Close()

	}()
	if !isExitHadoopCluster(namespaces, cName) {
		return
	}
	res, _ := stmt.Exec(namespaces, cName)
	_, _ = res.RowsAffected()
}

func getHadoopMasterNodeId(namespaces, cName string) (masterId string) {
	if dbHadoop == nil {
		connectDBHadoop()
	}
	rows, _ := dbHadoop.Query(`SELECT hadoop_master_id FROM hadoop_cluster WHERE namespace=? AND cluster_name=?`, namespaces, cName)
	defer func() {
		rows.Close()

	}()
	for rows.Next() {
		if err := rows.Scan(&masterId); err != nil {
			log.Fatal(err)
		}
	}
	return
}

//func getHadoopClusterStatus(namespaces, cName string) (clusterStatus string) {
//	if dbHadoop == nil {
//connectDBHadoop()
//}
//	rows, _ := dbHadoop.Query(`SELECT cluster_status FROM hadoop_cluster WHERE namespace=? AND cluster_name=?`, namespaces, cName)
//	defer func() {
//		rows.Close()
//
//	}()
//	for rows.Next() {
//		if err := rows.Scan(&clusterStatus); err != nil {
//			log.Fatal(err)
//		}
//	}
//	return
//}

func getHadoopNodeStatus(containerId string) (status string) {
	if dbHadoop == nil {
		connectDBHadoop()
	}
	rows, _ := dbHadoop.Query(`SELECT status FROM hadoop_nodes WHERE container_id=?`, containerId)
	defer func() {
		rows.Close()

	}()
	for rows.Next() {
		if err := rows.Scan(&status); err != nil {
			log.Fatal(err)
		}
	}
	return
}

func getHadoopSlaveNodeId(namespaces, cName string) (slaveIds []string) {
	if dbHadoop == nil {
		connectDBHadoop()
	}
	rows, _ := dbHadoop.Query(`SELECT hadoop_slave_id FROM hadoop_cluster WHERE namespace=? AND cluster_name=?`, namespaces, cName)
	defer func() {
		rows.Close()

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
	if dbHadoop == nil {
		connectDBHadoop()
	}
	stmt, _ := dbHadoop.Prepare(`DELETE FROM hadoop_nodes WHERE container_id=?`)
	defer func() {
		stmt.Close()

	}()
	res, _ := stmt.Exec(containerId)
	_, _ = res.RowsAffected()
}

func getNumberOfNodesbyNnameCname(namespaces, cName string) (numberOfNodes int) {
	if dbHadoop == nil {
		connectDBHadoop()
	}
	rows, _ := dbHadoop.Query(`SELECT number_of_nodes FROM hadoop_cluster WHERE namespace=? AND cluster_name=?`, namespaces, cName)
	defer func() {
		rows.Close()

	}()
	for rows.Next() {
		if err := rows.Scan(&numberOfNodes); err != nil {
			log.Fatal(err)
		}
	}
	return
}

func isExitHadoopCluster(namespaces, cName string) (flag bool) {
	if dbHadoop == nil {
		connectDBHadoop()
	}
	rows, _ := dbHadoop.Query(`SELECT number_of_nodes FROM hadoop_cluster WHERE namespace=? AND cluster_name=?`, namespaces, cName)
	defer func() {
		rows.Close()

	}()
	return rows.Next()
}

func (p *ZunProvider) ContainerHadoopNodeFactory(c *zun_container.Container, namespace, name, clusterType string) {
	mutex.Lock()
	defer mutex.Unlock()
	if c.Status == "Created" {
		tempName := name
		clusterName := tempName[0:strings.LastIndex(tempName[0:strings.LastIndex(tempName, "-")], "-")]
		//clusterName = clusterName[0:strings.LastIndex(clusterName, "-")]
		if getHadoopNodeStatus(c.UUID) == "Created" {
			return
		}
		if masterId := getHadoopMasterNodeId(namespace, clusterName); masterId == "" {
			//&& strings.Index(name, "master") != -1
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
			//clusterName = clusterName[0:strings.LastIndex(clusterName, "-")]
			updateHadoopNodeStatusToRunning(c.UUID)
			updatehadoopClusterAvailableNumber(namespace, clusterName)
			numberOfNode := getNumberOfNodesbyNnameCname(namespace, clusterName)
			AvailableNumber := getHadoopClusterAvailableNumber(namespace, clusterName)
			if numberOfNode == AvailableNumber {
				updatehadoopClusterCreateStatus(namespace, clusterName, "Creating")
				p.ConfigAndStartContainerHadoopCluster(namespace, clusterName, c.UUID, clusterType)
			} else if numberOfNode < AvailableNumber {
				p.addnodetocluster(namespace, clusterName, c.UUID)
			}
		}
	}
}

func (p *ZunProvider) ConfigAndStartContainerHadoopCluster(namespace, cName, container_id, clusterType string) {
	hadoopMasterId := getHadoopMasterNodeId(namespace, cName)
	hadoopSlaveIdList := getHadoopSlaveNodeId(namespace, cName)
	HostsStr, slavesStr := getFinallyHostsConfigAndSlavesConfig(namespace, cName)
	p.WriteHadoopConfigFile(hadoopMasterId, HostsStr, slavesStr)
	for _, v := range hadoopSlaveIdList {
		p.WriteHadoopConfigFile(v, HostsStr, slavesStr)
	}
	p.StartContainerHadoopCluster(hadoopMasterId,clusterType)
	updatehadoopClusterCreateStatus(namespace, cName, "Running")
}

func (p *ZunProvider) StartContainerHadoopCluster(containerId, clusterType string) {
	formatHdfsStr := fmt.Sprintf("hadoop namenode -format")
	formatHdfsCommand := zun_container.ExcuteOpts{
		//Command:"cat << EOF >   /etc/hosts  \n [global] \n EOF",
		Command:     formatHdfsStr,
		Run:         false,
		Interacitve: false,
	}
	formatHdfsResult, _ := zun_container.Exexcute(p.ZunClient, containerId, formatHdfsCommand).Extract()
	if formatHdfsResult.ExitCode !=0  {
		fmt.Println(formatHdfsResult.Output)
	}

	startHadoopClusterStr := fmt.Sprintf("/usr/local/hadoop/sbin/start-all.sh")
	startHadoopClusterCommand := zun_container.ExcuteOpts{
		//Command:"cat << EOF >   /etc/hosts  \n [global] \n EOF",
		Command:     startHadoopClusterStr,
		Run:         false,
		Interacitve: false,
	}
	startHadoopClusterResult, _ := zun_container.Exexcute(p.ZunClient, containerId, startHadoopClusterCommand).Extract()
	if startHadoopClusterResult.ExitCode !=0{
		fmt.Println(startHadoopClusterResult.Output)
	}
	if clusterType == "HADOOP"{
		fmt.Println("Hadoop Cluster created is success")
	}else if clusterType == "SPARK"{
		/*copySparkSlavesStr := fmt.Sprintf("cp /usr/local/hadoop/etc/hadoop/slaves  /usr/local/spark/conf/slaves ")
		copySparkSlavesCommand := zun_container.ExcuteOpts{
			//Command:"cat << EOF >   /etc/hosts  \n [global] \n EOF",
			Command:     copySparkSlavesStr,
			Run:         false,
			Interacitve: false,
		}
		copySparkSlavesResult, copySparkSlavesErr := zun_container.Exexcute(p.ZunClient, containerId, copySparkSlavesCommand).Extract()
		fmt.Printf("copySparkSlavesResult is %s\n",copySparkSlavesResult)
		fmt.Printf("copySparkSlavesErr is %s\n",copySparkSlavesErr)*/
		startSparkClusterStr := fmt.Sprintf("/usr/local/spark/conf/startSpark.sh")
		startSparkClusterCommand := zun_container.ExcuteOpts{
			//Command:"cat << EOF >   /etc/hosts  \n [global] \n EOF",
			Command:     startSparkClusterStr,
			Run:         false,
			Interacitve: false,
		}
		startSparkClusterResult, startSparkClusterErr := zun_container.Exexcute(p.ZunClient, containerId, startSparkClusterCommand).Extract()
		fmt.Printf("startSparkClusterResult is %s\n",startSparkClusterResult)
		fmt.Printf("startSparkClusterErr is %s\n",startSparkClusterErr)

	}
}

func (p *ZunProvider) addnodetocluster(namespace, cName, containerId string) {
	// Add numberOfNode
	updatehadoopClusterNumberofnodes(namespace, cName)
	hadoopMasterId := getHadoopMasterNodeId(namespace, cName)
	hadoopSlaveIdList := getHadoopSlaveNodeId(namespace, cName)
	HostsStr, slavesStr := getFinallyHostsConfigAndSlavesConfig(namespace, cName)
	p.WriteHadoopConfigFile(hadoopMasterId, HostsStr, slavesStr)
	for _, v := range hadoopSlaveIdList {
		p.WriteHadoopConfigFile(v, HostsStr, slavesStr)
	}

	startDatanodeCommand := zun_container.ExcuteOpts{
		Command:     fmt.Sprint("/usr/local/hadoop/sbin/hadoop-daemon.sh start datanode"),
		Run:         true,
		Interacitve: false,
	}
	startDatanodeResult, extractDatanodeErr := zun_container.Exexcute(p.ZunClient, containerId, startDatanodeCommand).Extract()
	if extractDatanodeErr != nil {
		fmt.Printf("extractDatanode err is %s", extractDatanodeErr)
	}
	if startDatanodeResult.ExitCode != 0 {
		fmt.Printf("startDatanode err is %s", startDatanodeResult.Output)
	}
	fmt.Println("add node to cluster success")
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
	nodestatus := getHadoopNodeStatus(containerId)
	if nodestatus != "Running" {
		return
	}
	HostsStr = "\"" + HostsStr + "\""
	slavesStr = "\"" + slavesStr + "\""
	hosts_slave := fmt.Sprintf("sh /root/modify.sh %s %s", HostsStr, slavesStr)
	modifyCommand := zun_container.ExcuteOpts{
		Command:     hosts_slave,
		Run:         true,
		Interacitve: false,
	}
	excuteResult, err := zun_container.Exexcute(p.ZunClient, containerId, modifyCommand).Extract()
	if err != nil {
		fmt.Printf("Extract modifyResult err:%s", err)
	}
	if excuteResult.ExitCode != 0 {
		fmt.Printf("excute modify hosts and slave err: %s", excuteResult.Output)
	}

}
func (p *ZunProvider) WriteHadoopConfigFile2(containerId, HostsStr, slavesStr string) {
	cpStr := fmt.Sprintf("cp /etc/hosts /etc/hosts.temp")
	cpCommand := zun_container.ExcuteOpts{
		//Command:"cat << EOF >   /etc/hosts  \n [global] \n EOF",
		Command:     cpStr,
		Run:         false,
		Interacitve: false,
	}
	_, err := zun_container.Exexcute(p.ZunClient, containerId, cpCommand).Extract()
	if err != nil {
		fmt.Printf("copy hosts.temp err is %v\n", err)
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
		fmt.Printf("Write HostsStr Err is %v\n", err)
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
		fmt.Printf("Write Hosts Err is %v\n", err)
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
		fmt.Printf("Write slaves Err is %v\n", err)
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
	if dbHadoop == nil {
		connectDBHadoop()
	}
	if isExitHadoopNode(node.containerId) {
		return nil
	}
	_, err := dbHadoop.Exec(
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
	if dbHadoop == nil {
		connectDBHadoop()
	}
	rows, _ := dbHadoop.Query(`SELECT container_id FROM hadoop_nodes WHERE container_id=?`, containerId)
	defer func() {
		rows.Close()

	}()
	return rows.Next()
}

func isRunningHadoopNode(containerId string) (flag bool) {
	if dbHadoop == nil {
		connectDBHadoop()
	}
	rows, _ := dbHadoop.Query(`SELECT status FROM hadoop_nodes WHERE container_id=?`, containerId)
	defer func() {
		rows.Close()

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
	if dbHadoop == nil {
		connectDBHadoop()
	}
	stmt, err := dbHadoop.Prepare("UPDATE hadoop_nodes SET status = ? WHERE container_id=?")
	if err != nil {
		log.Fatal(err)
	}
	_, err = stmt.Exec("Running", containerId)

}

func updatehadoopClusterAvailableNumber(namespaces, name string) {
	avaNumber := getHadoopClusterAvailableNumber(namespaces, name)
	avaNumber += 1
	if dbHadoop == nil {
		connectDBHadoop()
	}
	stmt, err := dbHadoop.Prepare("UPDATE hadoop_cluster SET available_number = ? WHERE namespace=? AND cluster_name=?")
	if err != nil {
		log.Fatal(err)
	}
	_, err = stmt.Exec(avaNumber, namespaces, name)
}

func updatehadoopClusterNumberofnodes(namespaces, name string) {
	if dbHadoop == nil {
		connectDBHadoop()
	}
	stmt, err := dbHadoop.Prepare("UPDATE hadoop_cluster SET number_of_nodes = number_of_nodes + 1 WHERE namespace=? AND cluster_name=?")
	if err != nil {
		log.Fatal(err)
	}
	_, err = stmt.Exec(namespaces, name)
}

func getHadoopClusterAvailableNumber(namespaces, cName string) (avaNumber int) {
	if dbHadoop == nil {
		connectDBHadoop()
	}
	rows, _ := dbHadoop.Query(`SELECT available_number FROM hadoop_cluster WHERE namespace=? AND cluster_name=?`, namespaces, cName)
	defer func() {
		rows.Close()

	}()
	for rows.Next() {
		if err := rows.Scan(&avaNumber); err != nil {
			log.Fatal(err)
		}
	}
	return
}

func updateHadoopClusterMasterId(masterId, namespaces, name string) {
	if dbHadoop == nil {
		connectDBHadoop()
	}
	stmt, err := dbHadoop.Prepare("UPDATE hadoop_cluster SET hadoop_master_id = ? WHERE namespace=? AND cluster_name=?")
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

	if dbHadoop == nil {
		connectDBHadoop()
	}
	stmt, err := dbHadoop.Prepare("UPDATE hadoop_cluster SET hadoop_slave_id = ? WHERE namespace=? AND cluster_name=?")
	if err != nil {
		log.Fatal(err)
	}
	_, err = stmt.Exec(slaveIds, namespaces, name)
}

func updatehadoopClusterCreateStatus(namespaces, name, status string) {
	if dbHadoop == nil {
		connectDBHadoop()
	}
	stmt, err := dbHadoop.Prepare("UPDATE hadoop_cluster SET cluster_status = ? WHERE namespace=? AND cluster_name=?")
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
	if dbHadoop == nil {
		connectDBHadoop()
	}
	rows, _ := dbHadoop.Query(`SELECT ip,container_id FROM hadoop_nodes WHERE container_id=?`, containerId)
	defer func() {
		rows.Close()

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
func getHadoopclusterStatus(namespaces, cName string) (clusterstatus string) {
	if dbHadoop == nil {
		connectDBHadoop()
	}
	rows, _ := dbHadoop.Query(`SELECT cluster_status FROM hadoop_cluster WHERE namespace=? AND cluster_name =?`, namespaces, cName)
	defer func() {
		rows.Close()

	}()
	for rows.Next() {
		if err := rows.Scan(&clusterstatus); err != nil {
			log.Fatal(err)
		}
	}
	return
}

func isMasterNode(containerId string) bool {
	if dbHadoop == nil {
		connectDBHadoop()
	}
	rows, _ := dbHadoop.Query(`SELECT node_type FROM hadoop_nodes WHERE container_id = ?`, containerId)
	defer func() {
		rows.Close()

	}()
	var node_type string
	for rows.Next() {
		if err := rows.Scan(&node_type); err != nil {
			log.Fatal(err)
		}
	}
	if node_type == "hadoop-master" {
		return true
	} else {
		return false
	}
	fmt.Println("isMasterNode func err")
	return false
}

func deletehadoopNode(namespaces, cName, containerId string) {
	mutex.Lock()
	defer mutex.Unlock()
	if dbHadoop == nil {
		connectDBHadoop()
	}
	ismaster := isMasterNode(containerId)

	// delete hadoop_node info in hadoop_nodes table
	stmtdeletenode, deletenodeerr := dbHadoop.Prepare("DELETE FROM hadoop_nodes WHERE container_id=?")
	if deletenodeerr != nil {
		log.Fatal(deletenodeerr)
	}
	_, stmtexecerr := stmtdeletenode.Exec(containerId)
	if stmtexecerr != nil {
		log.Fatal(stmtexecerr)
	}

	// if master node, delete cluster info in hadoop_cluster table
	if ismaster {
		stmtdeletemaster, deletemastererr := dbHadoop.Prepare("DELETE FROM hadoop_cluster WHERE namespace=? AND cluster_name=?")
		if deletemastererr != nil {
			log.Fatal(deletemastererr)
		}
		_, stmtexecerr := stmtdeletemaster.Exec(namespaces, cName)
		if stmtexecerr != nil {
			log.Fatal(stmtexecerr)
		}
	} else {
		//update hadoop_slave_id, avaNumber, number_of_nodes in hadoop_cluster table
		slaveList := getHadoopSlaveNodeId(namespaces, cName)
		slaveIds := ""
		if len(slaveList) > 1 {
			for _, v := range slaveList {
				if v != containerId {
					slaveIds = fmt.Sprintf("%s@-@%s", slaveIds, v)
				}
			}
			slaveIds = slaveIds[3:]
		}
		stmt, err := dbHadoop.Prepare("UPDATE hadoop_cluster SET hadoop_slave_id = ?, number_of_nodes = number_of_nodes - 1, available_number = available_number - 1 WHERE namespace=? AND cluster_name=?")
		if err != nil {
			log.Fatal(err)
		}
		_, err = stmt.Exec(slaveIds, namespaces, cName)
	}

	return
}
