package util

import (
	"encoding/json"
	"fmt"
	"log"
	"net/http"
)

func Error(w http.ResponseWriter, r *http.Request, httpStatus int, obj string) (err error) {
	return Json(w, r, httpStatus, map[string]string{
		"error": obj,
	})
}

func Json(w http.ResponseWriter, r *http.Request, httpStatus int, obj interface{}) (err error) {
	var bytes []byte
	if r.FormValue("pretty") != "" {
		bytes, err = json.MarshalIndent(obj, "", "  ")
	} else {
		bytes, err = json.Marshal(obj)
	}
	if err != nil {
		log.Printf("Marshal Error %v : %v", err, obj)
		return
	}
	callback := r.FormValue("callback")
	if callback == "" {
		w.Header().Set("Content-Type", "application/json")
		w.WriteHeader(httpStatus)
		_, err = w.Write(bytes)
		if err != nil {
			log.Printf("Write Error %v : %v", err, string(bytes))
			return
		}
	} else {
		w.Header().Set("Content-Type", "application/javascript")
		w.WriteHeader(httpStatus)
		if _, err = w.Write([]uint8(callback)); err != nil {
			log.Printf("Write1 Error %v", err)
			return
		}
		if _, err = w.Write([]uint8("(")); err != nil {
			log.Printf("Write2 Error %v", err)
			return
		}
		fmt.Fprint(w, string(bytes))
		if _, err = w.Write([]uint8(")")); err != nil {
			log.Printf("Write3 Error %v", err)
			return
		}
	}

	return
}
