package mapreduce

import (
	"encoding/json"
	"os"

	"github.com/sirupsen/logrus"
)

func doReduce(
	jobName string, // the name of the whole MapReduce job
	reduceTask int, // which reduce task this is
	outFile string, // write the output here
	nMap int, // the number of map tasks that were run ("M" in the paper)
	reduceF func(key string, values []string) string,
) {
	kvMap := make(map[string][]string)
	for i := 0; i < nMap; i++ {
		func() {
			fileName := reduceName(jobName, i, reduceTask)
			file, err := os.Open(fileName)
			if err != nil {
				logrus.WithError(err).Error("Open input file failed")
				return
			}
			defer file.Close()
			decoder := json.NewDecoder(file)
			kvList := make([]KeyValue, 0)
			err = decoder.Decode(&kvList)
			if err != nil {
				logrus.WithError(err).Error("Decode json failed")
				return
			}
			for _, kv := range kvList {
				_, ok := kvMap[kv.Key]
				if !ok {
					kvMap[kv.Key] = make([]string, 0)
				}
				kvMap[kv.Key] = append(kvMap[kv.Key], kv.Value)
			}
		}()
	}

	file, err := os.Create(outFile)
	if err != nil {
		logrus.WithError(err).Error("Create output file failed")
		return
	}
	defer file.Close()
	encoder := json.NewEncoder(file)
	for k, vList := range kvMap {
		v := reduceF(k, vList)
		err = encoder.Encode(KeyValue{
			Key:   k,
			Value: v,
		})
		if err != nil {
			logrus.WithError(err).Error("Write output file failed")
			return
		}
	}
}
