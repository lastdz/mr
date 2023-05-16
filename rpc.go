package mapreduce

type ExampleArgs struct {
	X int
}

type ExampleReply struct {
	Y int
}
type Getreq struct {
	X int
}
type Getresp struct {
	MFileName    string   //map文件名字
	TaskName     string   //任务名字
	RFileName    []string //reduce文件名字
	TaskType     int      //0:map,1:reduce,2:sleep
	ReduceNumber int
}
type ReportStatusRequest struct {
	FilesName []string //告诉coordinator，中间文件的名字，reduce用不上
	TaskName  string
}
type ReportStatusResponse struct {
	X int
}
