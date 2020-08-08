package main
import (
	"database/sql"
	"encoding/csv"
	"encoding/json"
	"fmt"
	"io"
	"log"
	"os"
	"strconv"
	"time"
	_ "github.com/go-sql-driver/mysql"
	client "github.com/influxdata/influxdb1-client/v2"
)
func queryDB(cli client.Client, cmd string) (res []client.Result, err error) {
	q := client.Query{
		Command:  cmd,
		Database: "to_be_finished",
	}
	if response, err := cli.Query(q); err == nil {
		if response.Error() != nil {
			return res, response.Error()
		}
		res = response.Results
	} else {
		return res, err
	}
	return res, nil
}
func connInflux(ip string) client.Client {
	cli, err := client.NewHTTPClient(client.HTTPConfig{
		Addr: fmt.Sprintf("http://%s:8086", ip),
		Username: "to_be_finished",
		Password: "to_be_finished",
	})
	if err != nil {
		panic(err.Error())
	}
	return cli
}
func connMysql(user, pwd, ip, port, db string)*sql.DB{
	DBDSN := fmt.Sprintf("%s:%s@tcp(%s:%s)/%s", user, pwd, ip, port, db)
	mysqlDB, err := sql.Open("mysql", DBDSN)
	//mysqlDB.SetConnMaxLifeTime()
	if err != nil {
		panic(err.Error())
	}
	return mysqlDB
}
func main() {
	beginTime := "2020-07-13T00:00:00.000Z"
	endTime := "2020-07-20T00:00:00.000Z"
	table := "to_be_finished"
	//csv
	newFileName := fmt.Sprintf("GPU有效使用率-%s-%s.csv", beginTime, endTime)
	nfs, err := os.Create(newFileName)
	if err != nil {
		log.Fatalf("can not create file, err is %+v", err)
	}
	defer nfs.Close()
	nfs.WriteString("\xEF\xBB\xBF")
	nfs.Seek(0, io.SeekEnd)
	w := csv.NewWriter(nfs)
	w.Comma = ','
	w.UseCRLF = true
	row := []string{"vm_id", "已用时长(min)", "有效使用时长(min)", "有效使用占比", "卡数", "平均gpu利用率(%)", "部门", "业务模块",
		"集群", "IP", "HostIP", "Creator", "Reminder", "创建时间", "status"}
	err = w.Write(row)
	if err != nil {
		log.Fatalf("can not write, err is %+v", err)
	}
	ip := "to_be_finished"
	//mysql
	cacheDB := connMysql("to_be_finished", "to_be_finished", ip, "3306", "to_be_finished")
	//influxdb
	conn := connInflux(ip)

	/*
	var count_3 string
	cnt_status := fmt.Sprintf("select COUNT(*) from t_vm where `status`=?")
	queryRow_cnt := cacheDB.QueryRow(cnt_status, "3")
	stat_err := queryRow_cnt.Scan(&count_3)
	if stat_err==nil{
		fmt.Println("Count = " + count_3)
	}
	return
	*/

	//query vmids
	queryVmID := fmt.Sprintf("select \"vm_id\",\"cluster\" from (select last(\"gpu_use_ratio\") from \"%s\" "+
		"where time > '%s' and time <= '%s' group by \"vm_id\" ,\"cluster\")", table, beginTime, endTime)
	resVmID, err := queryDB(conn, queryVmID)
	if err != nil {
		log.Fatal(err)
	}
	vms := resVmID[0].Series[0].Values
	iter := 0 //limit query frequency
	for _, row := range vms {
		vmID := row[1].(string)
		cluster := row[2].(string)

		// filter out the test vm id
		if len(vmID)<8{
			fmt.Println("less than 8: %s", vmID)
			continue
		}
		if vmID[:8] == "gpu-test"{
			continue
		}

		columnName := "vm_id"
		if vmID[:3]=="ts-"{
			columnName = "pod_name"
		}		
		// 先查status
		var vm_status string
		qs_status := fmt.Sprintf("select status from t_vm where %s=?",columnName)
		queryRow_status := cacheDB.QueryRow(qs_status, vmID)
		stat_err := queryRow_status.Scan(&vm_status)
		if stat_err != nil {
			fmt.Printf("remote tvm scan failed, VMID: %s, err:%v\n",vmID, stat_err)
		}

		// filtering vm_status
		if vm_status!="3"{
			fmt.Println(vmID + ",status=   "+vm_status)
			continue;
		}

		//fmt.Println(vmID)
		qs := fmt.Sprintf("select count(\"gpu_use_ratio\"), mean(\"gpu_use_ratio\")/100 from \"%s\" "+
			"where \"vm_id\" = '%s' and time > '%s' and time <= '%s'", table, vmID,  beginTime, endTime)
		res, err := queryDB(conn, qs)
		if err != nil {
			log.Fatal(err)
		}
		all, _ := res[0].Series[0].Values[0][1].(json.Number).Int64()
		gpuUseRatio := res[0].Series[0].Values[0][2].(json.Number).String()
		// filter
		gpuUseRatioValue, err := strconv.ParseFloat(gpuUseRatio, 32)
		if err != nil {
			log.Fatal(err)
		}

		qs = fmt.Sprintf("select count(\"gpu_use_ratio\") from \"%s\" "+
			"where \"vm_id\" = '%s' and \"gpu_use_ratio\" > 0 and time > '%s' and time <= '%s'", table, vmID,  beginTime, endTime)
		res, err = queryDB(conn, qs)
		if err != nil {
			log.Fatal(err)
		}
		var available int64 = 0
		if res[0].Series != nil {
			available, _ = res[0].Series[0].Values[0][1].(json.Number).Int64()
		}
		availableRatio := float32(available) / float32(all)
		// filter
		if (gpuUseRatioValue>1.0 && availableRatio >0.01){
			continue
		}

		//qs = fmt.Sprintf("select last(\"gpu_use_ratio\") from \"%s\" "+
		//	"where \"vm_id\" = '%s' and time > '%s' and time <= '%s' group by \"host_ip\",\"gpu_id\"", table, vmID, beginTime, endTime)
		//res, err = queryDB(conn, qs)
		//if err != nil {
		//	log.Fatal(err)
		//}
		//cards := strconv.Itoa(len(res[0].Series))
		
		//columnName := "vm_id"
		//if vmID[:3]=="ts-"{
		//	columnName = "pod_name"
		//}

		var dept, busi_module, ip, host_ip, gpu, creator, reminder, create_time, status string
		qs = fmt.Sprintf("select dept, busi_module, ip, host_ip, gpu, creator, reminder, create_time from t_vm where %s=?",columnName)
		queryRow := cacheDB.QueryRow(qs,vmID)
		err = queryRow.Scan(&dept, &busi_module, &ip, &host_ip, &gpu, &creator, &reminder, &create_time)
		if err != nil {
			fmt.Printf("remote tvm scan failed, VMID: %s, err:%v\n",vmID, err)
		}

		// filtering vm_status
		//if vm_status!="3"{
		//	continue;
		//}

		if host_ip != ""{
			queryRow = cacheDB.QueryRow("select status from t_host where ip=?", host_ip)
			err = queryRow.Scan(&status)
			if err != nil {
				fmt.Printf("remote thost scan failed, ip: %s, err:%v\n",host_ip, err)
			}
			//status abs
			if status != "" && status[0] == '-'{
				status = status[1:]
			}
			if status=="" || status[0]!='1' {
				continue;
			}
		}

		fmt.Println("Hit! " + vmID + ",status=   "+vm_status)
		row := []string{vmID, strconv.FormatInt(all, 10), strconv.FormatInt(available, 10),
			strconv.FormatFloat(float64(availableRatio), 'f', 3, 32), gpu, gpuUseRatio, dept, busi_module,
			cluster, ip, host_ip, creator, reminder, create_time, status}
		err = w.Write(row)

		w.Flush()

		if err != nil {
			log.Fatalf("can not write, err is %+v", err)
		}
		/*
		iter++
		if iter == 10000{
			w.Flush()
			time.Sleep(time.Duration(2)*time.Second)
			iter = 0
		}
		*/
	}
	w.Flush()
}