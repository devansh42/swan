package main

import (
	"net/http"
	"strconv"
)

func main() {
	http.HandleFunc("/write", write)
	http.HandleFunc("/read", read)
	http.ListenAndServe(":8000", nil)
}

func write(w http.ResponseWriter, r *http.Request) {
	name := r.FormValue("name")
	value := r.FormValue("value")
	fvalue, err := strconv.ParseFloat(value, 64)
	if len(name) == 0 || err != nil {
		w.WriteHeader(http.StatusBadRequest)
		return
	}
	writeInDB(name, fvalue) // Appends in timeseries database
}

func read(w http.ResponseWriter, r *http.Request) {

}

func writeInDB(name string, fvalue float64) {

}
