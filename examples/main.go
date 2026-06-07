package main

import (
	"fmt"
	"github.com/stratoberry/go-gpsd"
)

func main() {
	var gps *gpsd.Session
	var err error

	if gps, err = gpsd.Dial(gpsd.DefaultAddress); err != nil {
		panic(fmt.Sprintf("Failed to connect to GPSD: %s", err))
	}

	gps.AddFilter("TPV", func(r interface{}) {
		tpv := r.(*gpsd.TPVReport)
		fmt.Println("TPV", tpv.Mode, tpv.Time)
	})

	skyfilter := func(r interface{}) {
		sky := r.(*gpsd.SKYReport)

		fmt.Println("SKY", len(sky.Satellites), "satellites")
	}

	gps.AddFilter("SKY", skyfilter)

	err = gps.Watch()
	if err != nil {
		panic(fmt.Sprintf("Failed to watch GPSD: %s", err))
	}

	err = gps.Wait()
	if err != nil {
		panic(fmt.Sprintf("GPSD session error: %s", err))
	}
}
