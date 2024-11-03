package main

import (
	"encoding/json"
	"fmt"
	"os"
	"path/filepath"
	"sync"

	"github.com/jcelliott/lumber"
)

const Version = "1.0.1"

type (
	Logger interface {
		Fatal(string, ...interface{})
		Error(string, ...interface{})
		Warn(string, ...interface{})
		Info(string, ...interface{})
		Debug(string, ...interface{})
		Trace(string, ...interface{})
	}
	Driver struct { // driver -> intermediary between project & db
		mutex   sync.Mutex
		mutexes map[string]*sync.Mutex // string(name) -> contains pointer to a Mutex
		dir     string
		log     Logger
	}
)

func New(dir string, options *Options) (*Driver, error) {
	//creates a db and returns a driver
	dir = filepath.Clean(dir)
	opts := Options{}
	if options != nil {
		opts = *options
	}
	if opts.Logger == nil {
		opts.Logger = lumber.NewConsoleLogger((lumber.INFO)) // Uses INFO level for logging
	}
	driver := Driver{
		dir:     dir,
		mutexes: make(map[string]*sync.Mutex),
		log:     opts.Logger,
	}
	//now we check if database already exists.
	if _, err := os.Stat(dir); err == nil {
		opts.Logger.Debug("Using '%s' (db already exists) \n", dir)
		return &driver, nil
	}
	opts.Logger.Debug("Creating the db at '%s' ...\n", dir)
	return &driver, os.MkdirAll(dir, 0755)
	/*
		0755 is the access permission
			Commonly used on web servers. The owner can read, write, execute.
			Everyone else can read and execute but not modify the file.
	*/
}
func (d *Driver) Write(collection, resource string, v interface{}) error {
	if collection == "" {
		return fmt.Errorf("Missing collection - no place to save record!")
	}
	if resource == "" {
		return fmt.Errorf("Missing resource - no place to save record (no name)")
	}
	mutex := d.getOrCreateMutex(collection)
	mutex.Lock() //locked until write func completed
	defer mutex.Unlock()

	dir := filepath.Join(d.dir, collection)
	fnlPath := filepath.Join(dir, resource+".json")
	tmpPath := fnlPath + ".tmp"
	b, err := json.MarshalIndent(v, "", "\t")
	if err != nil {
		fmt.Println("err in write func\n")
		return err
	}
	if err := os.MkdirAll(dir, 0755); err != nil {
		fmt.Println("err in write func while creating dir\n")
		return err
	}
	b = append(b, byte('\n'))
	if err := os.WriteFile(tmpPath, b, 0644); err != nil {
		fmt.Println("err in write func while writing onto tmpPath\n")
		return err
	}
	return os.Rename(tmpPath, fnlPath) //these are not functions,but struct methods
}

func (d *Driver) Delete(collection, resource string) error {

	path := filepath.Join(collection, resource)
	mutex := d.getOrCreateMutex(collection)
	mutex.Lock()
	defer mutex.Unlock()

	dir := filepath.Join(d.dir, path)
	switch fi, err := stat(dir); {
	case fi == nil, err != nil:
		return fmt.Errorf("unable to find file/directory name %v", path)
	case fi.Mode().IsDir():
		return os.RemoveAll(dir)
	case fi.Mode().IsRegular():
		return os.RemoveAll(dir + ".json")
	}

	return nil
}
func (d *Driver) Read(collection, resource string, v interface{}) error {
	if collection == "" {
		return fmt.Errorf("Missing collection - no place to read record!")
	}
	if resource == "" {
		return fmt.Errorf("Missing resource - unable to read record (no name)")
	}
	record := filepath.Join(d.dir, collection, resource)
	if _, err := stat(record); err != nil {
		return err
	}
	b, err := os.ReadFile(record + ".json")
	if err != nil {
		return err
	}
	return json.Unmarshal(b, &v)
}

func (d *Driver) ReadAll(collection string) ([]string, error) {
	if collection == "" {
		return nil, fmt.Errorf("Missing collection!")
	}
	dir := filepath.Join(d.dir, collection)
	if _, err := stat(dir); err != nil {
		return nil, err
	}

	files, _ := os.ReadDir(dir)
	var records []string
	for _, file := range files {
		b, err := os.ReadFile(filepath.Join(dir, file.Name()))
		if err != nil {
			return nil, err
		}
		records = append(records, string(b))
	}
	return records, nil
}

func (d *Driver) getOrCreateMutex(collection string) *sync.Mutex {
	m, ok := d.mutexes[collection]
	if !ok {
		m = &sync.Mutex{}
		d.mutexes[collection] = m
	}
	return m
}

func stat(path string) (fi os.FileInfo, err error) { // Will check for both "path" and "path.json"
	fi, err = os.Stat(path) //first attempt to check with path
	if os.IsNotExist(err) {
		fi, err = os.Stat(path + ".json") //checks for the file with .json extension
		if err != nil {
			fmt.Println("stat func didnot find the path/path.json ->", path)
			return nil, err
		}
	}
	return fi, nil
}

type Options struct {
	Logger
}
type User struct {
	Name    string
	Age     json.Number
	Contact string
	Company string
	Address Address
	/*
	 Working with JSON from external sources
	 where number format isn't guaranteed - uses json.Number
	 Consider using json.Number in public APIs for maximum compatibility
	*/
}
type Address struct {
	City    string
	State   string
	Country string
	Pincode json.Number
}

func main() {
	dir := "./"
	db, err := New(dir, nil)
	if err != nil {
		fmt.Println("Error while creating new directory: ", err)
	}
	employees := []User{{
		"John", "34", "6003567434", "Deutsche Bank", Address{"Pune", "Maha", "India", "781394"},
	}, {
		"Ashlie", "23", "6006567434", "Axis Bank", Address{"New Delhi", "Delhi", "India", "781994"},
	}, {
		"Christy", "34", "6089567434", "Deutsche Bank", Address{"Pune", "Maha", "India", "781394"},
	}, {
		"Paul", "34", "9089567434", "HDFC Bank", Address{"Pune", "Maha", "India", "781394"},
	},
	}
	for _, value := range employees {
		db.Write("users", value.Name, User{
			Name:    value.Name,
			Age:     value.Age,
			Address: value.Address,
			Contact: value.Contact,
			Company: value.Company,
		})
	}

	records, err := db.ReadAll("users")
	if err != nil {
		fmt.Println("Error while reading: ", err)
	}
	fmt.Println(records)
	allUsers := []User{}
	for _, f := range records {
		employeeFound := User{}
		if err := json.Unmarshal([]byte(f), &employeeFound); err != nil {
			fmt.Println("error while reading -> ", err)
		}
		allUsers = append(allUsers, employeeFound)
	}
	fmt.Println((allUsers))

	if err := db.Delete("users", "Ashlie"); err != nil {
		fmt.Println("error while deleting -> ", err)
	}
	// delete one user

	if err := db.Delete("users", "Sam"); err != nil {
		fmt.Println("error while deleting -> ", err)
	}

	var recordFound User
	err = db.Read("users", "Ash", &recordFound)
	if err != nil {
		fmt.Println("err while searching for a name ->", err)
	} else if err == nil {
		fmt.Println("User with name found")
	}
	/*
		if err := db.Delete("users", ""); err != nil {
			fmt.Println("error while deleting -> ", err)
		}
		//delete all users
	*/

}
