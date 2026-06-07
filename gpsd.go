package gpsd

import (
	"bufio"
	"encoding/json"
	"errors"
	"fmt"
	"net"
	"os"
	"reflect"
	"sync"
	"time"
)

// ErrNotWatching not watching
var ErrNotWatching = errors.New("not watching")

// ErrTimedOut timed out
var ErrTimedOut = errors.New("timed out")

// ErrConnNil conn is nil
var ErrConnNil = errors.New("conn is nil")

// ErrConnClosed conn is closed
var ErrConnClosed = errors.New("conn is closed")

// DefaultAddress of gpsd (localhost:2947)
const DefaultAddress = "localhost:2947"

// Filter is a gpsd entry filter function
type Filter func(interface{})

// Session represents a connection to gpsd
type Session struct {
	socket  net.Conn
	reader  *bufio.Reader
	flock   sync.RWMutex
	filters map[string][]Filter
	cond    sync.Cond
	fErr    error
	timeout time.Duration
}

// Mode describes status of a TPV report
type Mode byte

const (
	// NoValueSeen indicates no data has been received yet
	NoValueSeen Mode = 0
	// NoFix indicates fix has not been required yet
	NoFix Mode = 1
	// Mode2D represents quality of the fix
	Mode2D Mode = 2
	// Mode3D represents quality of the fix
	Mode3D Mode = 3
)

type gpsdReport struct {
	Class string `json:"class"`
}

// TPVReport is a Time-Position-Velocity report
type TPVReport struct {
	Class  string    `json:"class"`
	Tag    string    `json:"tag"`
	Device string    `json:"device"`
	Mode   Mode      `json:"mode"`
	Time   time.Time `json:"time"`
	Ept    float64   `json:"ept"`
	Lat    float64   `json:"lat"`
	Lon    float64   `json:"lon"`
	Alt    float64   `json:"alt"`
	Epx    float64   `json:"epx"`
	Epy    float64   `json:"epy"`
	Epv    float64   `json:"epv"`
	Track  float64   `json:"track"`
	Speed  float64   `json:"speed"`
	Climb  float64   `json:"climb"`
	Epd    float64   `json:"epd"`
	Eps    float64   `json:"eps"`
	Epc    float64   `json:"epc"`
	Eph    float64   `json:"eph"`
}

// SKYReport reports sky view of GPS satellites
type SKYReport struct {
	Class      string      `json:"class"`
	Tag        string      `json:"tag"`
	Device     string      `json:"device"`
	Time       time.Time   `json:"time"`
	Xdop       float64     `json:"xdop"`
	Ydop       float64     `json:"ydop"`
	Vdop       float64     `json:"vdop"`
	Tdop       float64     `json:"tdop"`
	Hdop       float64     `json:"hdop"`
	Pdop       float64     `json:"pdop"`
	Gdop       float64     `json:"gdop"`
	Satellites []Satellite `json:"satellites"`
}

// GSTReport is pseudorange noise report
type GSTReport struct {
	Class  string    `json:"class"`
	Tag    string    `json:"tag"`
	Device string    `json:"device"`
	Time   time.Time `json:"time"`
	Rms    float64   `json:"rms"`
	Major  float64   `json:"major"`
	Minor  float64   `json:"minor"`
	Orient float64   `json:"orient"`
	Lat    float64   `json:"lat"`
	Lon    float64   `json:"lon"`
	Alt    float64   `json:"alt"`
}

// ATTReport reports vehicle-attitude from the digital compass or the gyroscope
type ATTReport struct {
	Class       string    `json:"class"`
	Tag         string    `json:"tag"`
	Device      string    `json:"device"`
	Time        time.Time `json:"time"`
	Heading     float64   `json:"heading"`
	MagSt       string    `json:"mag_st"`
	Pitch       float64   `json:"pitch"`
	PitchSt     string    `json:"pitch_st"`
	Yaw         float64   `json:"yaw"`
	YawSt       string    `json:"yaw_st"`
	Roll        float64   `json:"roll"`
	RollSt      string    `json:"roll_st"`
	Dip         float64   `json:"dip"`
	MagLen      float64   `json:"mag_len"`
	MagX        float64   `json:"mag_x"`
	MagY        float64   `json:"mag_y"`
	MagZ        float64   `json:"mag_z"`
	AccLen      float64   `json:"acc_len"`
	AccX        float64   `json:"acc_x"`
	AccY        float64   `json:"acc_y"`
	AccZ        float64   `json:"acc_z"`
	GyroX       float64   `json:"gyro_x"`
	GyroY       float64   `json:"gyro_y"`
	Depth       float64   `json:"depth"`
	Temperature float64   `json:"temperature"`
}

// VERSIONReport returns version details of gpsd client
type VERSIONReport struct {
	Class      string `json:"class"`
	Release    string `json:"release"`
	Rev        string `json:"rev"`
	ProtoMajor int    `json:"proto_major"`
	ProtoMinor int    `json:"proto_minor"`
	Remote     string `json:"remote"`
}

// DEVICESReport lists all devices connected to the system
type DEVICESReport struct {
	Class   string         `json:"class"`
	Devices []DEVICEReport `json:"devices"`
	Remote  string         `json:"remote"`
}

// DEVICEReport reports a state of a particular device
type DEVICEReport struct {
	Class     string  `json:"class"`
	Path      string  `json:"path"`
	Activated string  `json:"activated"`
	Flags     int     `json:"flags"`
	Driver    string  `json:"driver"`
	Subtype   string  `json:"subtype"`
	Bps       int     `json:"bps"`
	Parity    string  `json:"parity"`
	Stopbits  int     `json:"stopbits"`
	Native    int     `json:"native"`
	Cycle     float64 `json:"cycle"`
	Mincycle  float64 `json:"mincycle"`
}

// PPSReport is triggered on each pulse-per-second strobe from a device
type PPSReport struct {
	Class      string  `json:"class"`
	Device     string  `json:"device"`
	RealSec    float64 `json:"real_sec"`
	RealMusec  float64 `json:"real_musec"`
	ClockSec   float64 `json:"clock_sec"`
	ClockMusec float64 `json:"clock_musec"`
}

// TOFFReport is triggered on each PPS strobe from a device
type TOFFReport struct {
	Class     string  `json:"class"`
	Device    string  `json:"device"`
	RealSec   float64 `json:"real_sec"`
	RealNSec  float64 `json:"real_nsec"`
	ClockSec  float64 `json:"clock_sec"`
	ClockNSec float64 `json:"clock_nsec"`
}

// ERRORReport is an error response
type ERRORReport struct {
	Class   string `json:"class"`
	Message string `json:"message"`
}

// Satellite describes a location of a GPS satellite
type Satellite struct {
	PRN    float64 `json:"PRN"`
	Az     float64 `json:"az"`
	El     float64 `json:"el"`
	Ss     float64 `json:"ss"`
	Used   bool    `json:"used"`
	GnssId float64 `json:"gnssid"`
	SvId   float64 `json:"svid"`
	Health float64 `json:"health"`
}

// Attach create a session from a net.Conn
func Attach(conn net.Conn) (*Session, error) {
	if conn == nil {
		return nil, ErrConnNil
	}
	return dialCommon(conn, nil)
}

// Dial opens a new  ipv4 connection to GPSD.
func Dial(address string) (*Session, error) {
	return dialCommon(net.Dial("tcp4", address))
}

// DialIPv6 opens a new  ipv6 connection to GPSD.
func DialIPv6(address string) (*Session, error) {
	return dialCommon(net.Dial("tcp6", address))
}

// DialTimeout opens a new ipv4 connection to GPSD with a timeout for waiting to connect.
func DialTimeout(address string, to time.Duration) (*Session, error) {
	return dialCommon(net.DialTimeout("tcp4", address, to))
}

// DialIPv6Timeout opens a new ipv6 connection to GPSD with a timeout for waiting to connect.
func DialIPv6Timeout(address string, to time.Duration) (*Session, error) {
	return dialCommon(net.DialTimeout("tcp6", address, to))
}

func dialCommon(c net.Conn, err error) (session *Session, e error) {
	if err != nil {
		return nil, err
	}

	session = &Session{
		socket:  c,
		reader:  nil,
		filters: make(map[string][]Filter),
		cond:    sync.Cond{L: &sync.Mutex{}},
	}

	return
}

// Watch starts watching GPSD reports in a new goroutine without a timeout.
// Use Activate for a custom watch command
//
// Example:
//
//	gps := gpsd.Dial(gpsd.DEFAULT_ADDRESS)
//	_ := gpsd.Watch()
func (s *Session) Watch() error {
	return s.WatchWithTimeout(0)
}

// WatchWithTimeout starts watching GPSD reports in a new goroutine with a timeout.
// Use 0 for no timeout
// Use ActivateWithTimeout for a custom watch command
//
// Example:
//
//	gps := gpsd.Dial(gpsd.DEFAULT_ADDRESS)
//	_ := gpsd.Watch()
func (s *Session) WatchWithTimeout(timeout time.Duration) error {
	return s.ActivateWithTimeout("WATCH={\"enable\":true,\"json\":true,\"nmea\":false,\"raw\":0,\"scaled\":false,\"timing\":true,\"split24\":false,\"pps\":true}", timeout)
}

// Activate with a custom command unlike Watch with no timeout
func (s *Session) Activate(customCommand string) error {
	return s.ActivateWithTimeout(customCommand, 0)
}

// ActivateWithTimeout ith a custom command unlike Watch with a timeout (0 means no timeout)
func (s *Session) ActivateWithTimeout(customCommand string, timeout time.Duration) error {
	s.cond.L.Lock()
	defer s.cond.L.Unlock()

	if s.reader != nil {
		return nil
	}

	if s.socket == nil {
		return ErrConnClosed
	}

	s.reader = bufio.NewReader(s.socket)

	s.timeout = timeout

	var err error
	/*if timeout > 0 {
		_ = s.socket.SetReadDeadline(time.Now().Add(timeout))
	}
	_, err = s.reader.ReadString('\n')
	if err != nil {
	    defer func() {
			s.socket = nil
			s.reader = nil
		}()
		_ = s.socket.Close()
		return err
	}*/

	if timeout > 0 {
		_ = s.socket.SetWriteDeadline(time.Now().Add(timeout))
	}
	_, err = fmt.Fprintf(s.socket, "?"+customCommand+";")
	if err != nil {
		defer func() {
			s.socket = nil
			s.reader = nil
		}()
		_ = s.socket.Close()
		return err
	}

	go s.watch()

	return err
}

func (s *Session) IsWatching() bool {
	return s.socket != nil && s.reader != nil
}

func (s *Session) GetTimeout() time.Duration {
	return s.timeout
}

// Wait for the session to close returning an error if any, returns ErrNotWatching if Watch has not been run
func (s *Session) Wait() error {
	s.cond.L.Lock()
	defer s.cond.L.Unlock()
	if s.socket == nil {
		return ErrConnClosed
	}
	if s.reader == nil {
		return ErrNotWatching
	}
	s.cond.Wait()
	return s.fErr
}

// FinalError gets the value of the error that would be returned by Wait
func (s *Session) FinalError() error {
	return s.fErr
}

// SendCommand sends a command to GPSD
func (s *Session) SendCommand(command string) error {
	s.cond.L.Lock()
	defer s.cond.L.Unlock()
	if s.socket == nil {
		return ErrConnClosed
	}
	if s.timeout > 0 {
		_ = s.socket.SetWriteDeadline(time.Now().Add(s.timeout))
	}
	_, err := fmt.Fprintf(s.socket, "?"+command+";")
	if errors.Is(err, os.ErrDeadlineExceeded) {
		return ErrTimedOut
	}
	return err
}

// AddFilter attaches a function which will be called for all
// GPSD reports with the given class. Callback functions have type Filter.
//
// Example:
//
//	gps := gpsd.Init(gpsd.DEFAULT_ADDRESS)
//	gps.AddFilter("TPV", func (r interface{}) {
//	  report := r.(*gpsd.TPVReport)
//	  fmt.Println(report.Time, report.Lat, report.Lon)
//	})
//	_ = gps.Watch()
func (s *Session) AddFilter(class string, f Filter) {
	s.flock.Lock()
	defer s.flock.Unlock()
	s.filters[class] = append(s.filters[class], f)
}

// RemoveFilter removes a specified filter instance added by AddFilter of a specified class
func (s *Session) RemoveFilter(class string, f Filter) {
	s.flock.Lock()
	defer s.flock.Unlock()
	n := make([]Filter, 0, len(s.filters[class]))
	for _, filter := range s.filters[class] {
		if reflect.ValueOf(filter).Pointer() != reflect.ValueOf(f).Pointer() {
			n = append(n, filter)
		}
	}
	s.filters[class] = n
}

// ClearFilters clears all filters added by AddFilter of a specified class
func (s *Session) ClearFilters(class string) {
	s.flock.Lock()
	defer s.flock.Unlock()
	s.filters[class] = []Filter{}
}

func (s *Session) deliverReport(class string, report interface{}) {
	s.flock.RLock()
	defer s.flock.RUnlock()
	for _, f := range s.filters[class] {
		f(report)
	}
}

func (s *Session) filterCount(class string) int {
	s.flock.RLock()
	defer s.flock.RUnlock()
	return len(s.filters[class])
}

// Close closes the connection to GPSD
func (s *Session) Close() error {
	s.cond.L.Lock()
	defer s.cond.L.Unlock()
	if s.socket == nil {
		return ErrConnClosed
	}

	err := s.socket.Close()

	s.socket = nil
	if s.reader == nil {
		s.cond.Broadcast()
	} else {
		if err != nil {
			return err
		}
		s.cond.Wait()
	}
	return err
}

func (s *Session) watch() {
	// We're not using a JSON decoder because we first need to inspect
	// the JSON string to determine it's "class"
	defer func() {
		s.cond.L.Lock()
		defer s.cond.L.Unlock()
		defer s.cond.Broadcast()
		if s.socket != nil {
			defer func() { s.socket = nil }()
			_ = s.socket.Close()
		}
	}()
	for {
		if s.timeout > 0 {
			_ = s.socket.SetReadDeadline(time.Now().Add(s.timeout))
		}
		if line, err := s.reader.ReadString('\n'); err == nil {
			var reportPeek gpsdReport
			lineBytes := []byte(line)
			if err = json.Unmarshal(lineBytes, &reportPeek); err == nil {
				if s.filterCount(reportPeek.Class) == 0 {
					continue
				}

				if report, err2 := unmarshalReport(reportPeek.Class, lineBytes); err2 == nil {
					s.deliverReport(reportPeek.Class, report)
				} else {
					s.fErr = fmt.Errorf("JSON parsing error 2: %w", err)
					break
				}
			} else {
				s.fErr = fmt.Errorf("JSON parsing error: %w", err)
				break
			}
		} else {
			if !errors.Is(err, net.ErrClosed) {
				s.fErr = fmt.Errorf("stream reader error (is gpsd running?): %w", err)
			}
			if errors.Is(err, os.ErrDeadlineExceeded) {
				s.fErr = ErrTimedOut
			}
			break
		}
	}
}

func unmarshalReport(class string, bytes []byte) (interface{}, error) {
	var err error

	switch class {
	case "TPV":
		var r *TPVReport
		err = json.Unmarshal(bytes, &r)
		return r, err
	case "SKY":
		var r *SKYReport
		err = json.Unmarshal(bytes, &r)
		return r, err
	case "GST":
		var r *GSTReport
		err = json.Unmarshal(bytes, &r)
		return r, err
	case "ATT":
		var r *ATTReport
		err = json.Unmarshal(bytes, &r)
		return r, err
	case "VERSION":
		var r *VERSIONReport
		err = json.Unmarshal(bytes, &r)
		return r, err
	case "DEVICE":
		var r *DEVICEReport
		err = json.Unmarshal(bytes, &r)
		return r, err
	case "DEVICES":
		var r *DEVICESReport
		err = json.Unmarshal(bytes, &r)
		return r, err
	case "PPS":
		var r *PPSReport
		err = json.Unmarshal(bytes, &r)
		return r, err
	case "TOFF":
		var r *TOFFReport
		err = json.Unmarshal(bytes, &r)
		return r, err
	case "ERROR":
		var r *ERRORReport
		err = json.Unmarshal(bytes, &r)
		return r, err
	}

	return nil, err
}
