package main

import (
	"bufio"
	"encoding/json"
	"flag"
	"fmt"
	"io"
	"io/ioutil"
	"math/rand"
	"os"
	"os/exec"
	"regexp"
	"strconv"
	"strings"
	"time"

	log "github.com/sirupsen/logrus"
	"gopkg.in/yaml.v2"
)

type ConfigOption struct {
	Name       string
	Type       string
	StartValue string
	Min        float64
	Max        float64
}

// Value definition as returned by 'ceph config show osd.0'
//
//	in JSON format
type CurrentConfigValue struct {
	Name   string
	Value  string
	Source string
}

var r = rand.New(rand.NewSource(time.Now().UnixNano()))
var restartOSDs bool
var configFile, benchType string
var timeout, confSleep, benchTime, poolPGs, benchScale, benchBlockSize, benchObjectSize int

func init() {
	flag.BoolVar(&restartOSDs, "restart-OSD", false, "Add this to restart OSDs when necessary to apply new configuration")
	flag.StringVar(&configFile, "conf", "test.yaml", "Location of the config file listing ceph config options to try out")
	flag.IntVar(&timeout, "timeout", 30, "Numbers of unsuccessful optimization attempts until stopping")
	flag.IntVar(&confSleep, "conf-sleep", 2, "Seconds to wait after applying the a new config option")
	flag.IntVar(&benchTime, "bench-time", 30, "Benchmark length in seconds")
	flag.IntVar(&poolPGs, "pool-pgs", 64, "pg_num and pgp_num to use for testbench pool creation")
	flag.StringVar(&benchType, "bench-type", "write", "Benchmark type - one of write,seq,rand")
	flag.IntVar(&benchScale, "bench-scale", 4, "Number of concurrent IOs in benchmark")
	flag.IntVar(&benchBlockSize, "bench-block-size", 4000, "Benchmark Block IO size in KB")
	flag.IntVar(&benchObjectSize, "bench-object-size", 4000, "Benchmark Object IO size in KB")
}

func main() {
	flag.Parse()
	// Create a new logger for writing logs to a file
	logFile, err := os.OpenFile("debug.log", os.O_CREATE|os.O_WRONLY|os.O_APPEND, 0666)
	if err != nil {
		log.Fatal(err)
	}
	defer logFile.Close()
	log.SetOutput(ioutil.Discard) // Send all logs to nowhere by default

	log.AddHook(&WriterHook{ // Send logs with level higher than warning to stderr
		Writer: logFile,
		LogLevels: []log.Level{
			log.PanicLevel,
			log.FatalLevel,
			log.ErrorLevel,
			log.WarnLevel,
			log.InfoLevel,
			log.DebugLevel,
		},
	})
	log.AddHook(&WriterHook{ // Send info and debug logs to stdout
		Writer: os.Stdout,
		LogLevels: []log.Level{
			log.PanicLevel,
			log.FatalLevel,
			log.ErrorLevel,
			log.WarnLevel,
			log.InfoLevel,
		},
	})
	// Set the logger instance to be used globally
	// log.SetOutput(io.MultiWriter(logFile, os.Stdout)) // Writes logs to both file and stdout
	log.SetLevel(log.DebugLevel) // Set the global log level to Debug

	optionsFile, _ := os.ReadFile(configFile)

	var optionList []ConfigOption
	var bestConfig []CurrentConfigValue
	var highestScore float64 = 0

	if err := yaml.Unmarshal([]byte(optionsFile), &optionList); err != nil {
		log.WithError(err).Fatal("Unmarshal error for config list")
	}

	if len(optionList) == 0 {
		log.WithField("options", optionList).Fatal("You need to supply at least one config option")
		return
	}
	printConfigOptionList(optionList)

	setUpCephPool()

	for _, option := range optionList {
		setValueToStart(&option)
	}

	for noNewBest := 0; noNewBest < timeout; noNewBest++ {
		option := getRandOption(optionList)
		oldValue := getCurrentValueForOption(option)
		newValue := findNewValueForOption(option)
		setValue(&option, newValue)
		log.Debugf("Setting %s to %s - old value was %s", option.Name, newValue, oldValue)

		newScore, err := getScore()
		if err != nil {
			log.WithError(err).Fatal("Cannot get new score - exiting")
		}
		if newScore > highestScore {
			highestScore = newScore
			log.Info("Found new best config!")
			log.WithFields(log.Fields{"tunedOption": option.Name, "newValue": newValue}).Infof("New Avg IOPs %d", int(highestScore))
			bestConfig = getCurrentConfig()
			noNewBest = 0
		} else {
			log.Info("No new best config")
			setValue(&option, oldValue)
		}
		time.Sleep(time.Duration(confSleep) * time.Second)
	}
	log.Infof("Search has ended after %d tries without finding a better config", timeout)
	printBestConfig(bestConfig)
	removeCephPool()
}

func getCurrentConfig() []CurrentConfigValue {
	output, err := executeCommand("/usr/bin/ceph", strings.Split("config show osd.0 -f json", " "))
	if err != nil {
		log.WithError(err).Error("Cannot execute ceph command to get current config for OSD.0")
		return []CurrentConfigValue{}
	}
	var currentConfig []CurrentConfigValue

	if err := json.Unmarshal([]byte(output), &currentConfig); err != nil {
		log.WithError(err).Error("Cannot get current config of OSD.0 as template")
		return []CurrentConfigValue{}
	}
	return currentConfig
}

func getCurrentValueForOption(option ConfigOption) (value string) {
	output, err := executeCommand("/usr/bin/ceph", []string{"config", "get", "osd.0", option.Name})
	if err != nil {
		log.WithError(err).Errorf("Cannot execute ceph command to get current value for %s", option.Name)
		return ""
	}
	return output
}

func getRandOption(options []ConfigOption) ConfigOption {
	randomIndex := r.Intn(len(options))
	return options[randomIndex]
}

func findNewValueForOption(option ConfigOption) (value string) {
	if option.Type == "bool" {
		return fmt.Sprint(r.Intn(2) == 0)
	}
	valueRange := option.Max - option.Min
	// check if Max or Min are actually integer
	if option.Max == float64(int64(option.Max)) && option.Min == float64(int64(option.Min)) {
		return fmt.Sprint(r.Int63n(int64(valueRange)) + int64(option.Min))
	}
	return fmt.Sprint(option.Min + r.Float64()*(option.Max-option.Min))
}

func setValue(option *ConfigOption, value string) {
	_, err := executeCommand("/usr/bin/ceph", []string{"tell", "osd.*", "injectargs", fmt.Sprintf("--%s=%s", option.Name, value)})
	if err != nil {
		log.WithError(err).Errorf("Issues setting value %s to %s", option.Name, value)
	}
}

func setValueToStart(option *ConfigOption) {
	if option.StartValue == "" {
		return
	}
	setValue(option, option.StartValue)
}

func setUpCephPool() {
	executeCommand("/usr/bin/ceph", []string{"osd", "pool", "create", "testbench", fmt.Sprint(poolPGs), fmt.Sprint(poolPGs)})
	executeCommand("/usr/bin/ceph", strings.Split("osd pool application enable testbench rbd", " "))
}
func removeCephPool() {
	executeCommand("/usr/bin/ceph", strings.Split("tell mon.* injectargs --mon_allow_pool_delete true", " "))
	executeCommand("/usr/bin/ceph", strings.Split("osd pool delete testbench testbench --yes-i-really-really-mean-it", " "))
}

func getScore() (number float64, err error) {
	output, err := executeCommand("/usr/bin/rados", []string{"bench", "-p", "testbench", fmt.Sprint(benchTime), "write", "-t", fmt.Sprint(benchScale), "-b", fmt.Sprint(benchBlockSize * 1024), "-O", fmt.Sprint(benchObjectSize * 1024)})
	if err != nil {
		log.WithError(err).Error("Error getting score!")
	}

	// Define the string to search for
	searchString := "Average IOPS"

	// Regex to match integers and float values
	pattern := `[-+]?[0-9]*\.?[0-9]+`
	re := regexp.MustCompile(pattern)

	// Create a scanner to read the output line by line
	scanner := bufio.NewScanner(strings.NewReader(output))

	// Iterate through each line of the output
	for scanner.Scan() {
		line := scanner.Text()

		// Check if the line contains the desired string
		if strings.Contains(line, searchString) {
			match := re.FindString(line)
			number, err := strconv.ParseFloat(match, 64)
			if err != nil {
				log.WithError(err).Error("Error extracting score")
			}
			return number, nil
		}
	}

	// Check for any scanner errors
	if err := scanner.Err(); err != nil {
		log.Fatal(err)
	}
	return 0, fmt.Errorf("could not find score in output")
}

func executeCommand(command string, arguments []string) (output string, err error) {
	// Execute the command
	cmd := exec.Command(command, arguments...)

	// Capture the output
	cmdoutput, err := cmd.Output()
	if err != nil && fmt.Sprint(err) != "exit status 22" {
		log.WithError(err).WithField("stdOut", cmdoutput).Fatalf("Issues executing command %s %s", command, strings.Join(arguments, " "))
		return "", err
	}
	return string(cmdoutput), nil
}

func printConfigOptionList(options []ConfigOption) {
	var names []string
	for _, option := range options {
		names = append(names, option.Name)
	}
	log.WithField("options", names).Info("All config options that will be used to optimize Ceph")
}

func printBestConfig(configOptions []CurrentConfigValue) {
	out := ""
	for _, option := range configOptions {
		out += fmt.Sprintf("%s = %s\n", option.Name, option.Value)
	}
	log.Infof("Best config is:\n%s", out)
}

// WriterHook is a hook that writes logs of specified LogLevels to specified Writer
type WriterHook struct {
	Writer    io.Writer
	LogLevels []log.Level
}

// Fire will be called when some logging function is called with current hook
// It will format log entry to string and write it to appropriate writer
func (hook *WriterHook) Fire(entry *log.Entry) error {
	line, err := entry.String()
	if err != nil {
		return err
	}
	_, err = hook.Writer.Write([]byte(line))
	return err
}

// Levels define on which log levels this hook would trigger
func (hook *WriterHook) Levels() []log.Level {
	return hook.LogLevels
}
