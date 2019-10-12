package mapreduce

import (
	"encoding/json"
	"hash/fnv"
	"io/ioutil"
	"os"

	"github.com/sirupsen/logrus"
)

func doMap(
	jobName string, // the name of the MapReduce job
	mapTask int, // which map task this is
	inFile string,
	nReduce int, // the number of reduce task that will be run ("R" in the paper)
	mapF func(filename string, contents string) []KeyValue,
) {
	iFile, err := os.Open(inFile)
	if err != nil {
		logrus.WithError(err).Error("Open input file failed")
		return
	}
	defer iFile.Close()
	bytes, err := ioutil.ReadAll(iFile)
	if err != nil {
		logrus.WithError(err).Error("Read intput file failed")
		return
	}
	kvList := mapF(inFile, string(bytes))
	kvMat := make([][]KeyValue, nReduce)
	for i := 0; i < nReduce; i++ {
		kvMat[i] = make([]KeyValue, 0)
	}
	for _, kv := range kvList {
		index := ihash(kv.Key) % nReduce
		kvMat[index] = append(kvMat[index], kv)
	}
	for i := 0; i < nReduce; i++ {
		func() {
			fileName := reduceName(jobName, mapTask, i)
			oFile, err := os.Create(fileName)
			if err != nil {
				logrus.WithError(err).Error("Create output file failed")
				return
			}
			defer oFile.Close()
			encoder := json.NewEncoder(oFile)
			err = encoder.Encode(kvMat[i])
			if err != nil {
				logrus.WithError(err).Error("Write output file failed")
				return
			}
		}()
	}
}

func ihash(s string) int {
	h := fnv.New32a()
	_, _ = h.Write([]byte(s))
	return int(h.Sum32() & 0x7fffffff)
}
