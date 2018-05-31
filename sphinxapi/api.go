package sphinxapi

import (
	"bytes"
	"encoding/binary"
	"errors"
	"fmt"
	"log"
	"math"
	"net"
	"os"
	//"encoding/hex"
)

const (
	// known searchd status codes
	SEARCHD_OK      = 0
	SEARCHD_ERROR   = 1
	SEARCHD_RETRY   = 2
	SEARCHD_WARNING = 3
)

const (
	// known match modes
	SPH_MATCH_ALL       = 0
	SPH_MATCH_ANY       = 1
	SPH_MATCH_PHRASE    = 2
	SPH_MATCH_BOOLEAN   = 3
	SPH_MATCH_EXTENDED  = 4
	SPH_MATCH_FULLSCAN  = 5
	SPH_MATCH_EXTENDED2 = 6
)

const (
	// known ranking modes (extended2 mode only)
	SPH_RANK_PROXIMITY_BM25 = 0 // default mode, phrase proximity major factor and BM25 minor one
	SPH_RANK_BM25           = 1 // statistical mode, BM25 ranking only (faster but worse quality)
	SPH_RANK_NONE           = 2 // no ranking, all matches get a weight of 1
	SPH_RANK_WORDCOUNT      = 3 // simple word-count weighting, rank is a weighted sum of per-field keyword occurence counts
	SPH_RANK_PROXIMITY      = 4
	SPH_RANK_MATCHANY       = 5
	SPH_RANK_FIELDMASK      = 6
	SPH_RANK_SPH04          = 7
	SPH_RANK_EXPR           = 8
	SPH_RANK_TOTAL          = 9
)

const (
	// known sort modes
	SPH_SORT_RELEVANCE     = 0
	SPH_SORT_ATTR_DESC     = 1
	SPH_SORT_ATTR_ASC      = 2
	SPH_SORT_TIME_SEGMENTS = 3
	SPH_SORT_EXTENDED      = 4
	SPH_SORT_EXPR          = 5
)

const (
	// known filter types
	SPH_FILTER_VALUES     = 0
	SPH_FILTER_RANGE      = 1
	SPH_FILTER_FLOATRANGE = 2
	SPH_FILTER_STRING     = 3
)

const (
	// known attribute types
	SPH_ATTR_NONE      = 0
	SPH_ATTR_INTEGER   = 1
	SPH_ATTR_TIMESTAMP = 2
	SPH_ATTR_ORDINAL   = 3
	SPH_ATTR_BOOL      = 4
	SPH_ATTR_FLOAT     = 5
	SPH_ATTR_BIGINT    = 6
	SPH_ATTR_STRING    = 7
	SPH_ATTR_MULTI     = 0x40000001
	SPH_ATTR_MULTI64   = 0x40000002
)

const (
	// known grouping funcs
	SPH_GROUPBY_DAY      = 0
	SPH_GROUPBY_WEEK     = 1
	SPH_GROUPBY_MONTH    = 2
	SPH_GROUPBY_YEAR     = 3
	SPH_GROUPBY_ATTR     = 4
	SPH_GROUPBY_ATTRPAIR = 5
)

const (
	// known searchd commands
	SEARCHD_COMMAND_SEARCH     = 0
	SEARCHD_COMMAND_EXCERPT    = 1
	SEARCHD_COMMAND_UPDATE     = 2
	SEARCHD_COMMAND_KEYWORDS   = 3
	SEARCHD_COMMAND_PERSIST    = 4
	SEARCHD_COMMAND_STATUS     = 5
	SEARCHD_COMMAND_FLUSHATTRS = 7
)

const (
	// current client-side command implementation versions
	VER_COMMAND_SEARCH     = 0x11E
	VER_COMMAND_EXCERPT    = 0x104
	VER_COMMAND_UPDATE     = 0x103
	VER_COMMAND_KEYWORDS   = 0x101
	VER_COMMAND_STATUS     = 0x101
	VER_COMMAND_QUERY      = 0x100
	VER_COMMAND_FLUSHATTRS = 0x100

	//SPH_ATTR_TYPES = [
	//	SPH_ATTR_NONE,
	//	SPH_ATTR_INTEGER,
	//	SPH_ATTR_TIMESTAMP,
	//	SPH_ATTR_ORDINAL,
	//	SPH_ATTR_BOOL,
	//	SPH_ATTR_FLOAT,
	//	SPH_ATTR_BIGINT,
	//	SPH_ATTR_STRING,
	//	SPH_ATTR_MULTI,
	//	SPH_ATTR_MULTI64
	//]
)

type SphinxFilter struct {
	Attr       string
	FilterType int
	NumValues  int
	Values     []int
	UMin       int64
	UMax       int64
	FMin       float32
	FMax       float32
	Exclude    int
	SValue     string
}

type SphinxResultAttr struct {
	Name string
	Type int
}

type SphinxResult struct {
	Error   string
	Warning string
	Status  int

	NumFields int
	Fields     []string

	Attrs []SphinxResultAttr

	NumMatches int
	Matches []interface{}
	ValuesPool []interface{}

	Total      int
	TotalFound int
	TimeMsec   int
	NumWords   int
	Words      []SphinxWordinfo
}

type SphinxOverride struct {
	attr        string
	docids      []uint64
	num_values  int
	uint_values []uint
}

type SphinxAnchor struct {
	attrlat  string
	attrlong string
	lat      float32
	long     float32
}

type SphinxWordinfo struct {
	Word string
	Docs int
	Hits int
}

func packUInt64(number uint64) []byte {
	buf := make([]byte, 8)
	binary.BigEndian.PutUint64(buf, number)
	return buf
}

func packUInt32(number uint32) []byte {
	buf := make([]byte, 4)
	binary.BigEndian.PutUint32(buf, number)
	return buf
}

func packFloat32(f float32) []byte {
	var buf bytes.Buffer
	err := binary.Write(&buf, binary.BigEndian, f)
	if err != nil {
		fmt.Println("binary.Write failed:", err)
	}
	return buf.Bytes()
}

/**
* SphinxClient Object
*
* @api public
 */
type SphinxClient struct {
	_host          string // searchd host (default is "localhost")
	_port          int    // searchd port (default is 9312)
	_path          string // searchd unix-domain socket path
	_socket        interface{}
	_offset        int                       // how much records to seek from result-set start (default is 0)
	_limit         int                       // how much records to return from result-set starting at offset (default is 20)
	_mode          int                       // query matching mode (default is SPH_MATCH_ALL)
	_weights       []int                     // per-field weights (default is 1 for all fields)
	_sort          int                       // match sorting mode (default is SPH_SORT_RELEVANCE)
	_sortby        string                    // attribute to sort by (defualt is "")
	_min_id        int                       // min ID to match (default is 0)
	_max_id        int                       // max ID to match (default is UINT_MAX)
	_filters       []SphinxFilter            // search filters
	_groupby       string                    // group-by attribute name
	_groupfunc     int                       // group-by func (to pre-process group-by attribute value with)
	_groupsort     string                    // group-by sorting clause (to sort groups in result set with)
	_groupdistinct string                    // group-by count-distinct attribute
	_maxmatches    int                       // max matches to retrieve
	_cutoff        int                       // cutoff to stop searching at
	_retrycount    int                       // distributed retry count
	_retrydelay    int                       // distributed retry delay
	_anchor        SphinxAnchor              // geographical anchor point
	_indexweights  map[string]int            // per-index weights
	_ranker        int                       // ranking mode
	_rankexpr      string                    // ranking expression for SPH_RANK_EXPR
	_maxquerytime  int                       // max query time, milliseconds (default is 0, do not limit)
	_timeout       float64                   // connection timeout
	_fieldweights  map[string]int            // per-field-name weights
	_overrides     map[string]SphinxOverride // per-query attribute values overrides
	_select        string                    // select-list (attributes or expressions, with optional aliases)
	_persist		bool					 // keep persist
	_client *net.TCPConn
	_error   string        // last error message
	_warning string        // last warning message
	_reqs    *[]interface{} // requests array for multi-query
}

func CreateSphinxClient() SphinxClient {
	c := SphinxClient{
		_host:          "localhost",
		_port:          9312,
		_path:          "",
		_socket:        nil,
		_offset:        0,
		_limit:         20,
		_mode:          SPH_MATCH_EXTENDED2, //SPH_MATCH_ALL,
		_weights:       []int{},
		_sort:          SPH_SORT_RELEVANCE,
		_sortby:        "",
		_min_id:        0,
		_max_id:        0,
		_filters:       []SphinxFilter{},
		_groupby:       "",
		_groupfunc:     SPH_GROUPBY_DAY,
		_groupsort:     "@groupby desc",
		_groupdistinct: "",
		_maxmatches:    1000,
		_cutoff:        0,
		_retrycount:    0,
		_retrydelay:    0,
		_anchor:        SphinxAnchor{},
		_indexweights:  map[string]int{},
		_ranker:        SPH_RANK_PROXIMITY_BM25,
		_rankexpr:      "",
		_maxquerytime:  0,
		_timeout:       1.0,
		_fieldweights:  map[string]int{},
		_overrides:     map[string]SphinxOverride{},
		_select:        "",

		_error:   "",
		_warning: "",
		_reqs:    &[]interface{}{},
	}
	return c
}

func (c *SphinxClient) get_connection() *net.TCPConn {
	tcpAddr, err := net.ResolveTCPAddr("tcp", fmt.Sprintf("%s:%d", c._host, c._port))
	if err != nil {
		println("ResolveTCPAddr failed:", err.Error())
		os.Exit(1)
	}

	client, err := net.DialTCP("tcp", nil, tcpAddr)
	if err != nil {
		println("Dial failed:", err.Error())
		os.Exit(1)
	}

	// now send major client protocol version
	_, err = client.Write(packUInt32(1))
	if err != nil {
		println("Write to server failed:", err.Error())
		os.Exit(1)
	}

	reply := make([]byte, 1024)
	_, err = client.Read(reply)
	if err != nil {
		println("Write to server failed:", err.Error())
		os.Exit(1)
	}

	return client
}
/**
* Sets and sends request packet to searchd server.
*
* @api private
 */
func (c *SphinxClient) _sendRequest(client_ver int, request []byte, fn func(err error, response []byte)) {
	//log.Printf("Connecting to %s:%d", c._host, c._port)
	var client *net.TCPConn
	if (!c._persist) {
		client = c.get_connection();
	} else {
		client = c._client
	}

	reply := make([]byte, 1024)

	// sent request
	_, err := client.Write(request)
	if err != nil {
		println("Write to server failed:", err.Error())
		os.Exit(1)
	}

	_length, err := client.Read(reply)
	if err != nil {
		println("Write to server failed:", err.Error())
		os.Exit(1)
	}

	chunk := reply[0:_length]

	header_buf := chunk[0:32]

	//fmt.Println(hex.Dump(header_buf))

	var content []byte
	var state, version int16
	var length int32

	// chunking
	for {
		if content == nil {
			// read header
			state = int16(binary.BigEndian.Uint16(header_buf[0:2]))
			version = int16(binary.BigEndian.Uint16(header_buf[2:4]))
			length = int32(binary.BigEndian.Uint32(header_buf[4:8]))
			content = append(content, chunk[8:]...)
		} else {
			_length, err = client.Read(reply)
			if err != nil {
				println("Write to server failed:", err.Error())
				os.Exit(1)
			}

			chunk = reply[0:_length]
			content = append(content, chunk...)
		}

		if len(content) >= int(length) {
			switch state {
			case SEARCHD_OK:
				break
			case SEARCHD_WARNING:
				fmt.Println("WARNING")
				//	var wend = 4 + unpack(">L", content)
				//	warning = string(content[4 : wend])
				//	// TODO do something with the warning !!!
				break
			case SEARCHD_ERROR:
				fmt.Println("SERVER RETURN ERROR:", string(content[4:]))
				err = errors.New("searchd error: " + string(content[4:]))
				content = nil
				break
			case SEARCHD_RETRY:
				fmt.Println("RETRY")
				err = errors.New("temporary searchd error: " + string(content[4:]))
				content = nil
				break
			default:
				fmt.Println("UNKNOWN")
				err = errors.New(fmt.Sprintf("unknown status code %s", int(state)))
				content = nil
				break
			}

			if int(version) < client_ver {
				fmt.Printf("ERROR: searchd command v.%d.%d older than client\"s v.%d.%d, some options might not work\n",
					version>>8, version&0xff, client_ver>>8, client_ver&0xff)
				//	c._warning = fmt.Sprintf("searchd command v.%d.%d older than client\"s v.%d.%d, some options might not work",
				//		version>>8, version&0xff, client_ver>>8, client_ver&0xff)
				//	// TODO do something with the warning !!!
			}

			if (!c._persist) {
				client.Close()
			}
			fn(err, content)
			return
		}
	}
}

func (c *SphinxClient) GetLastError() string {
	return c._error
}

func (c *SphinxClient) GetLastWarning() string {
	return c._warning
}

/**
* Set searchd server host and port.
*
* @api public
 */
func (c *SphinxClient) SetServer(host string, port int) {
	c._host = host
	c._port = port
}

func (c *SphinxClient) SetConnectTimeout(timeout float64) {
	c._timeout = math.Max(0.001, timeout)
}

func (c *SphinxClient) SetLimits(offset int, limit int, maxmatches int, cutoff int) {

	c._offset = offset
	c._limit = limit

	c._maxmatches = maxmatches
	c._cutoff = cutoff
}

func (c *SphinxClient) SetMaxQueryTime(maxquerytime int) {
	c._maxquerytime = maxquerytime
}

func (c *SphinxClient) SetMatchMode(mode int) {
	//var modes = [SPH_MATCH_ALL, SPH_MATCH_ANY, SPH_MATCH_PHRASE, SPH_MATCH_BOOLEAN, SPH_MATCH_EXTENDED, SPH_MATCH_FULLSCAN, SPH_MATCH_EXTENDED2]
	// TODO CHECK IT IN modes
	c._mode = mode
}

func (c *SphinxClient) SetRankingMode(ranker int, rankexpr string) {
	//assert(0 <= ranker && ranker < SPH_RANK_TOTAL)
	c._ranker = ranker
	c._rankexpr = rankexpr
}

func (c *SphinxClient) SetSortMode(mode int, clause string) {

	//var modes = [SPH_SORT_RELEVANCE, SPH_SORT_ATTR_DESC, SPH_SORT_ATTR_ASC, SPH_SORT_TIME_SEGMENTS, SPH_SORT_EXTENDED, SPH_SORT_EXPR]
	// TODO check mode in modes
	c._sort = mode
	c._sortby = clause
}

func (c *SphinxClient) SetWeights(weights []int) {
	c._weights = weights
}

func (c *SphinxClient) SetFieldWeights(weights map[string]int) {
	c._fieldweights = weights
}

func (c *SphinxClient) SetIndexWeights(weights map[string]int) {
	c._indexweights = weights
}

func (c *SphinxClient) SetIDRange(minid int, maxid int) {
	// TODO assert(minid <= maxid)

	c._min_id = minid
	c._max_id = maxid
}

func (c *SphinxClient) SetFilter(attribute string, values []int, exclude int) {
	c._filters = append(c._filters, SphinxFilter{
		FilterType: SPH_FILTER_VALUES,
		Attr:       attribute,
		Exclude:    exclude,
		Values:     values,
	})
}

func (c *SphinxClient) SetFilterString(attribute string, value string, exclude int) {
	c._filters = append(c._filters, SphinxFilter{
		FilterType: SPH_FILTER_STRING,
		Attr:       attribute,
		Exclude:    exclude,
		SValue:     value,
	})
}

func (c *SphinxClient) SetFilterRange(attribute string, min_ int64, max_ int64, exclude int) {

	// TODO assert(min_<=max_)
	c._filters = append(c._filters, SphinxFilter{
		FilterType: SPH_FILTER_RANGE,
		Attr:       attribute,
		Exclude:    exclude,
		UMin:       min_,
		UMax:       max_,
	})
}

func (c *SphinxClient) SetFilterFloatRange(attribute string, min_ float32, max_ float32, exclude int) {

	// TODO assert(min_<=max_)

	c._filters = append(c._filters, SphinxFilter{
		FilterType: SPH_FILTER_FLOATRANGE,
		Attr:       attribute,
		Exclude:    exclude,
		FMin:       min_,
		FMax:       max_,
	})
}

func (c *SphinxClient) SetGeoAnchor(attrlat string, attrlong string, latitude float32, longitude float32) {
	c._anchor = SphinxAnchor{
		attrlat:  attrlat,
		attrlong: attrlong,
		lat:      latitude,
		long:     longitude,
	}
}

func (c *SphinxClient) SetGroupBy(attribute string, groupFunc int, groupsort string) {

	if groupsort == "" {
		groupsort = "@group desc"
	}

	//var funcs = [SPH_GROUPBY_DAY, SPH_GROUPBY_WEEK, SPH_GROUPBY_MONTH, SPH_GROUPBY_YEAR, SPH_GROUPBY_ATTR, SPH_GROUPBY_ATTRPAIR]
	// TODO groupFunc in funcs
	c._groupby = attribute
	c._groupfunc = groupFunc
	c._groupsort = groupsort
}

func (c *SphinxClient) SetGroupDistinct(attribute string) {
	c._groupdistinct = attribute
}

func (c *SphinxClient) SetRetries(count int, delay int) {
	//TODO assert(count >= 0)
	//TODO assert(delay >= 0)
	c._retrycount = count
	c._retrydelay = delay
}

//func (c *SphinxClient) SetOverride(name string, ovverideType int, values map[string]interface{}) {
//	// TODO assert(SPH_ATTR_TYPES.some(func (x) { return (x == ovverideType) }))
//	c._overrides[name] = SphinxOverride{
//		"name": name,
//		"type": ovverideType,
//		"values": values,
//	}
//}

func (c *SphinxClient) SetSelect(value string) {
	c._select = value
}

func (c *SphinxClient) ResetOverrides() {

	c._overrides = map[string]SphinxOverride{}
}

func (c *SphinxClient) ResetFilters() {

	c._filters = []SphinxFilter{}
	c._anchor = SphinxAnchor{}
}

func (c *SphinxClient) ResetGroupBy() {
	c._groupby = ""
	c._groupfunc = SPH_GROUPBY_DAY
	c._groupsort = "@group desc"
	c._groupdistinct = ""
}

/**
* Connect to searchd server and run given search query.
*
* @api public
 */
func (c *SphinxClient) Query(query string, index string, comment string, fn func(error, interface{})) {

	//if (len(arguments) == 2) {
	//	fn = arguments[1];
	//	index = "*";
	//	comment = "";
	//}
	//else if (len(arguments) == 3) {
	//	fn = arguments[2];
	//	comment = "";
	//}

	c.AddQuery(query, index, comment)
	c.RunQueries(func(err error, results interface{}) {
		c._reqs = &[]interface{}{} // we won"t re-run erroneous batch

		if err != nil {
			fn(err, nil)
			return
		}

		//c._error = results[0].error
		//c._warning = results[0].warning
		//if (results[0].status == SEARCHD_ERROR) {
		//	fn(results[0].error, nil)
		//	return
		//}
		fn(err, results)
	})

}

/**
* Add query to batch.
* index can be "*"
* @api public
 */
func (c *SphinxClient) AddQuery(query string, index_list string, comment string) int {

	var req = []byte{}

	req = append(req, packUInt32(uint32(64))...) // if client ver_search >=0x11B

	req = append(req, packUInt32(uint32(c._offset))...)
	req = append(req, packUInt32(uint32(c._limit))...)
	req = append(req, packUInt32(uint32(c._mode))...)
	req = append(req, packUInt32(uint32(c._ranker))...)

	if c._ranker == SPH_RANK_EXPR {
		req = append(req, packUInt32(uint32(len(c._rankexpr)))...)
		req = append(req, []byte(c._rankexpr)...)
	}

	req = append(req, packUInt32(uint32(c._sort))...)

	req = append(req, packUInt32(uint32(len(c._sortby)))...)
	req = append(req, []byte(c._sortby)...)
	// TODO : check if query is encoding in utf8

	req = append(req, packUInt32(uint32(len(query)))...)
	req = append(req, []byte(query)...)

	req = append(req, packUInt32(uint32(len(c._weights)))...)

	for item := range c._weights {
		req = append(req, packUInt32(uint32(item))...)
	}

	req = append(req, packUInt32(uint32(len(index_list)))...)
	req = append(req, []byte(index_list)...)

	req = append(req, packUInt32(uint32(1))...) // id64 range bits

	req = append(req, packUInt64(uint64(c._min_id))...)
	req = append(req, packUInt64(uint64(c._max_id))...)

	// filters
	req = append(req, packUInt32(uint32(len(c._filters)))...)
	for _, filter := range c._filters {
		fmt.Println("FILTER")
		req = append(req, packUInt32(uint32(len(filter.Attr)))...)
		req = append(req, []byte(filter.Attr)...)
		var filtertype = filter.FilterType
		req = append(req, packUInt32(uint32(filtertype))...)
		if filtertype == SPH_FILTER_VALUES {
			req = append(req, packUInt32(uint32(len(filter.Values)))...)
			for _, val := range filter.Values {
				req = append(req, packUInt64(uint64(val))...)
			}
		} else if filtertype == SPH_FILTER_RANGE {
			req = append(req, packUInt64(uint64(filter.UMin))...)
			req = append(req, packUInt64(uint64(filter.UMax))...)
		} else if filtertype == SPH_FILTER_FLOATRANGE {
			req = append(req, packFloat32(filter.FMin)...)
			req = append(req, packFloat32(filter.FMax)...)
		} else if filtertype == SPH_FILTER_STRING {
			req = append(req, packUInt32(uint32(len(filter.SValue)))...)
			req = append(req, []byte(filter.SValue)...)
		}
		req = append(req, packUInt32(uint32(filter.Exclude))...)
	}

	// group-by, max-matches, group-sort
	req = append(req, packUInt32(uint32(c._groupfunc))...)

	req = append(req, packUInt32(uint32(len(c._groupby)))...)
	req = append(req, []byte(c._groupby)...)

	req = append(req, packUInt32(uint32(c._maxmatches))...)

	req = append(req, packUInt32(uint32(len(c._groupsort)))...)
	req = append(req, []byte(c._groupsort)...)

	req = append(req, packUInt32(uint32(c._cutoff))...)
	req = append(req, packUInt32(uint32(c._retrycount))...)
	req = append(req, packUInt32(uint32(c._retrydelay))...)

	req = append(req, packUInt32(uint32(len(c._groupdistinct)))...)
	req = append(req, []byte(c._groupdistinct)...)

	// anchor point
	if c._anchor.attrlat == "" && c._anchor.attrlong == "" {
		req = append(req, packUInt32(uint32(0))...)
	} else {
		req = append(req, packUInt32(uint32(1))...)
		req = append(req, packUInt32(uint32(len(c._anchor.attrlat)))...)
		req = append(req, []byte(c._anchor.attrlat)...)
		req = append(req, packUInt32(uint32(len(c._anchor.attrlong)))...)
		req = append(req, []byte(c._anchor.attrlong)...)
		req = append(req, packFloat32(c._anchor.lat)...)
		req = append(req, packFloat32(c._anchor.long)...)
	}

	// per-index_list weights
	req = append(req, packUInt32(uint32(len(c._indexweights)))...)

	for index, weight := range c._indexweights {
		req = append(req, packUInt32(uint32(len(index)))...)
		req = append(req, []byte(index)...)

		req = append(req, packUInt32(uint32(weight))...)
	}

	// max query time
	req = append(req, packUInt32(uint32(c._maxquerytime))...)

	// per-field weights
	req = append(req, packUInt32(uint32(len(c._fieldweights)))...)
	for field, weight := range c._fieldweights {
		req = append(req, packUInt32(uint32(len(field)))...)
		req = append(req, []byte(field)...)

		req = append(req, packUInt32(uint32(weight))...)
	}

	// comment
	req = append(req, packUInt32(uint32(len(comment)))...)
	req = append(req, []byte(comment)...)

	// attribute overrides
	req = append(req, packUInt32(uint32(len(c._overrides)))...)
	for _, override := range c._overrides {
		req = append(req, packUInt32(uint32(len(override.attr)))...)
		req = append(req, []byte(override.attr)...)

		req = append(req, packUInt32(uint32( /*override["type"].(int)*/ SPH_ATTR_INTEGER))...)

		req = append(req, packUInt32(uint32(len(override.uint_values)))...)
		for _, value := range override.uint_values {
			fmt.Println("VALUE", value)
			//req = append( req, packUInt64(uint64(id))...)
			//
			//if (override["type"] == SPH_ATTR_FLOAT) {
			//	req = append( req, packFloat64( value )...)
			//} else if (override["type"] == SPH_ATTR_BIGINT) {
			//	req = append( req, packUInt32(uint32(id))...)
			//} else {
			//	req = append( req, []byte(value)...)
			//}
		}
	}

	// select-list
	req = append(req, packUInt32(uint32(len(c._select)))...)
	req = append(req, []byte(c._select)...)

	//req = append(req, packUInt32(uint32(0))...) // predicted_time
	req = append(req, packUInt32(uint32(0))...) // outer_orderby
	req = append(req, packUInt32(uint32(0))...) // outer_offset
	req = append(req, packUInt32(uint32(0))...) // outer_limit
	req = append(req, packUInt32(uint32(0))...) // has_outer



	// send query, get response
	*c._reqs = append(*(c._reqs), req)

	return len(*c._reqs) - 1
}

/**
* Run queries batch.
* Returns None on network IO failure; or an array of result set hashes on success.
* @api public
 */
func (c *SphinxClient) RunQueries(fn func(error, interface{})) {

	var nreqs = len(*c._reqs)

	if nreqs == 0 {
		c._error = "no queries defined, issue AddQuery() first"
		return
	}

	length := 8
	for _,req := range *c._reqs {
		length += len(req.([]byte))
	}

	var header bytes.Buffer
	binary.Write(&header, binary.BigEndian, uint16(SEARCHD_COMMAND_SEARCH))
	binary.Write(&header, binary.BigEndian, uint16(VER_COMMAND_SEARCH))
	binary.Write(&header, binary.BigEndian, uint32(length))
	binary.Write(&header, binary.BigEndian, uint32(0))
	binary.Write(&header, binary.BigEndian, uint32(len(*c._reqs)))

	var request []byte
	request = append(request, header.Bytes()...)

	for _,req := range *c._reqs {
		request = append(request, req.([]byte)...)
	}

	c._sendRequest(VER_COMMAND_SEARCH, request, func(err error, response []byte) {

		if (response == nil) {
			fn(err, nil)
		} else {
			// parse response
			max_ := len(response)
			p := 0
			results := []SphinxResult{}
			for i := 0; i < nreqs; i++ {
				result := SphinxResult{}

				result.Error = ""
				result.Warning = ""
				result.Status = int(binary.BigEndian.Uint32(response[p : p+4]))
				p += 4

				if result.Status != SEARCHD_OK {
					length = int(binary.BigEndian.Uint32(response[p : p+4]))
					p += 4
					var message = response[p : p+length]
					p += length

					if result.Status == SEARCHD_WARNING {
						result.Warning = string(message)
					} else {
						result.Error = string(message)
						results = append(results, result)
						continue
					}
				}

				// read schema
				result.Fields = []string{}
				var attrs = []SphinxResultAttr{}

				var nfields = int(binary.BigEndian.Uint32(response[p : p+4]))
				p += 4
				for nfields > 0 && p < max_ {
					nfields -= 1
					length = int(binary.BigEndian.Uint32(response[p : p+4]))
					p += 4
					result.Fields = append(result.Fields, string(response[p:p+length]))
					p += length
				}

				var nattrs = int(binary.BigEndian.Uint32(response[p : p+4]))

				p += 4
				for nattrs > 0 && p < max_ {
					nattrs -= 1
					length = int(binary.BigEndian.Uint32(response[p : p+4]))
					p += 4
					var attr = string(response[p : p+length])
					p += length
					var type_ = int(binary.BigEndian.Uint32(response[p : p+4]))
					p += 4
					attrs = append(attrs, SphinxResultAttr{
						Name: attr,
						Type: type_})
				}

				result.Attrs = attrs

				// read match count
				var num_matches = int(binary.BigEndian.Uint32(response[p : p+4]))
				result.NumMatches = num_matches
				p += 4
				var id64 = int(binary.BigEndian.Uint32(response[p : p+4]))
				p += 4

				// read matches
				for num_matches > 0 && p < max_ {
					var doc, weight int
					num_matches -= 1
					if id64 > 0 {
						doc = int(binary.BigEndian.Uint64(response[p : p+8]))
						p += 8
						weight = int(binary.BigEndian.Uint32(response[p : p+4]))
						p += 4
					} else {
						doc = int(binary.BigEndian.Uint32(response[p : p+4]))
						p += 4
						weight = int(binary.BigEndian.Uint32(response[p : p+4]))
						p += 4
					}

					var match = map[string]interface{}{
						"id":     doc,
						"weight": weight,
						"attrs":  map[int]interface{}{},
					}


					for jdx := 0;  jdx < len(result.Attrs); jdx++ {
						attr := attrs[jdx]

						attributes := map[string]interface{}{}
						if (attr.Type == SPH_ATTR_FLOAT) {
							fmt.Println("GOT FLOAT")
							attributes[attr.Name] = float32(binary.BigEndian.Uint32(response[p: p + 4]))
						} else if (attr.Type == SPH_ATTR_BIGINT) {
							attributes[attr.Name] = int(binary.BigEndian.Uint64(response[p : p + 8]))
							p += 4
						} else if (attr.Type == SPH_ATTR_STRING) {
							var slen = int(binary.BigEndian.Uint32(response[p : p + 4]))
							p += 4
							attributes[attr.Name] = ""
							if (slen>0) {
								attributes[attr.Name] = string(response[p : p + slen])
							}
							p += slen-4
						} else if (attr.Type == SPH_ATTR_MULTI) {
							attributes[attr.Name] = []int{}
							var nvals = int(binary.BigEndian.Uint32(response[p : p + 4]))
							p += 4
							for n := 0; n < nvals; n++ {
								attributes[attr.Name] = append( attributes[attr.Name].([]int), int(binary.BigEndian.Uint32(response[p: p + 4])))
								p += 4
							}
							p -= 4
						} else if (attr.Type == SPH_ATTR_MULTI64) {
							attributes[attr.Name] = []int{}
							nvals := int(binary.BigEndian.Uint32(response[p: p + 4]))
							nvals = nvals/2
							p += 4
							for n := 0; n < nvals; n++ {
								attributes[attr.Name] = append( attributes[attr.Name].([]int), int(binary.BigEndian.Uint64(response[p: p + 8])))
								p += 8
							}
							p -= 4
						} else {
							attributes[attr.Name] = int(binary.BigEndian.Uint32(response[p: p + 4]))
						}
						p += 4
						match["attrs"] = attributes
					}

					result.Matches = append( result.Matches, match )
				}
				result.Total = int(binary.BigEndian.Uint32(response[p : p+4]))
				p += 4
				result.TotalFound = int(binary.BigEndian.Uint32(response[p : p+4]))
				p += 4
				result.TimeMsec = int(binary.BigEndian.Uint32(response[p : p+4]))
				p += 4
				var words = int(binary.BigEndian.Uint32(response[p : p+4]))
				p += 4

				//result["time"] = (result["time"].(float64)/1000.0)

				result.Words = []SphinxWordinfo{}
				for words > 0 {
					words -= 1
					var length = int(binary.BigEndian.Uint32(response[p : p+4]))
					p += 4
					word := string(response[p : p+length])
					p += length
					docs := int(binary.BigEndian.Uint32(response[p : p+4]))
					p += 4
					hits := int(binary.BigEndian.Uint32(response[p : p+4]))
					p += 4

					result.Words = append(result.Words, SphinxWordinfo{
						Word: word,
						Docs: docs,
						Hits: hits,
					})
				}
				results = append(results, result)
			}

			fn(err, results)
		}
	})

	c._reqs = &[]interface{}{}
}

type Options map[string]interface{}

func (c Options) Repair() {

	if _, ok := c["before_match"]; !ok {
		c["before_match"] = "<b>"
	}

	if _, ok := c["after_match"]; !ok {
		c["after_match"] = "</b>"
	}

	if _, ok := c["chunk_separator"]; !ok {
		c["chunk_separator"] = " ... "
	}

	if _, ok := c["limit"]; !ok {
		c["limit"] = 256
	}

	if _, ok := c["limit_passages"]; !ok {
		c["limit_passages"] = 0
	}

	if _, ok := c["limit_words"]; !ok {
		c["limit_words"] = 0
	}

	if _, ok := c["around"]; !ok {
		c["around"] = 5
	}

	if _, ok := c["exact_phrase"]; !ok {
		c["exact_phrase"] = false
	}

	if _, ok := c["single_passage"]; !ok {
		c["single_passage"] = false
	}

	if _, ok := c["use_boundaries"]; !ok {
		c["use_boundaries"] = false
	}

	if _, ok := c["weight_order"]; !ok {
		c["weight_order"] = false
	}

	if _, ok := c["query_mode"]; !ok {
		c["query_mode"] = false
	}

	if _, ok := c["force_all_words"]; !ok {
		c["force_all_words"] = false
	}

	if _, ok := c["start_passage_id"]; !ok {
		c["start_passage_id"] = 1
	}

	if _, ok := c["load_files"]; !ok {
		c["load_files"] = false
	}

	if _, ok := c["html_strip_mode"]; !ok {
		c["html_strip_mode"] = "index"
	}

	if _, ok := c["allow_empty"]; !ok {
		c["allow_empty"] = false
	}

	if _, ok := c["passage_boundary"]; !ok {
		c["passage_boundary"] = "none"
	}

	if _, ok := c["emit_zones"]; !ok {
		c["emit_zones"] = false
	}

	if _, ok := c["foo"]; !ok {
		c["load_files_scattered"] = false
	}
}

func (c *SphinxClient) BuildExcerpts(docs []string, index string, words string, opts Options, cb func(error, interface{})) {

	opts.Repair()

	var flags = 1 // remove spaces
	if opts["exact_phrase"].(bool) {
		flags = flags | 2
	}

	if opts["single_passage"].(bool) {
		flags = flags | 4
	}

	if opts["use_boundaries"].(bool) {
		flags = flags | 8
	}

	if opts["weight_order"].(bool) {
		flags = flags | 16
	}

	if opts["query_mode"].(bool) {
		flags = flags | 32
	}

	if opts["force_all_words"].(bool) {
		flags = flags | 64
	}

	if opts["load_files"].(bool) {
		flags = flags | 128
	}

	if opts["allow_empty"].(bool) {
		flags = flags | 256
	}

	if opts["emit_zones"].(bool) {
		flags = flags | 512
	}

	if opts["load_files_scattered"].(bool) {
		flags = flags | 1024
	}

	req := []byte{}

	req = append(req, packUInt32(uint32(0))...)
	req = append(req, packUInt32(uint32(flags))...)
	req = append(req, packUInt32(uint32(len(index)))...)
	req = append(req, []byte(index)...)
	req = append(req, packUInt32(uint32(len(words)))...)
	req = append(req, []byte(words)...)

	req = append(req, packUInt32(uint32(len(opts["before_match"].(string))))...)
	req = append(req, []byte(opts["before_match"].(string))...)
	req = append(req, packUInt32(uint32(len(opts["after_match"].(string))))...)
	req = append(req, []byte(opts["after_match"].(string))...)
	req = append(req, packUInt32(uint32(len(opts["chunk_separator"].(string))))...)
	req = append(req, []byte(opts["chunk_separator"].(string))...)
	req = append(req, packUInt32(uint32(opts["limit"].(int)))...)
	req = append(req, packUInt32(uint32(opts["around"].(int)))...)
	req = append(req, packUInt32(uint32(opts["limit_passages"].(int)))...)
	req = append(req, packUInt32(uint32(opts["limit_words"].(int)))...)
	req = append(req, packUInt32(uint32(opts["start_passage_id"].(int)))...)
	req = append(req, packUInt32(uint32(len(opts["html_strip_mode"].(string))))...)
	req = append(req, []byte(opts["html_strip_mode"].(string))...)
	req = append(req, packUInt32(uint32(len(opts["passage_boundary"].(string))))...)
	req = append(req, []byte(opts["passage_boundary"].(string))...)
	req = append(req, packUInt32(uint32(len(docs)))...)

	for i := 0; i < len(docs); i++ {
		var doc = docs[i]
		// TODO assert.equal(typeof doc, "string")
		req = append(req, packUInt32(uint32(len(doc)))...)
		req = append(req, []byte(doc)...)
	}

	var length = len(req)

	log.Println("Build excerpts request:", string(req))

	var header bytes.Buffer
	binary.Write(&header, binary.BigEndian, uint16(SEARCHD_COMMAND_EXCERPT))
	binary.Write(&header, binary.BigEndian, uint16(VER_COMMAND_EXCERPT))
	binary.Write(&header, binary.BigEndian, uint32(length))

	var request []byte
	request = append(request, header.Bytes()...)
	request = append(request, req...)

	fn := func(err error, response []byte) {
		if err != nil {
			cb(err, nil)
		} else {
			var results = []interface{}{}
			p := 0
			rlen := len(response)
			for i := 0; i < len(docs); i++ {
				len := []interface{}{int(binary.BigEndian.Uint32(response[p : p+4]))}[0].(int)
				p += 4
				if p+len > rlen {
					cb(errors.New("Incomplete reply from searchd"), nil)
				}

				if len > 0 {
					results = append(results, response[p:p+len])
				} else {
					results = append(results, "")
				}
				p += len
			}
			cb(nil, results)
		}
	}

	c._sendRequest(VER_COMMAND_EXCERPT, request, fn)
}

func (c *SphinxClient) UpdateAttributes(index string, attrs interface{}, values interface{}, mva interface{}) {
}

func (c *SphinxClient) BuildKeywords(query string, index string, hits bool, cb func(error, interface{})) {

	var req = []byte{}

	req = append(req, packUInt32(uint32(len(query)))...)
	req = append(req, []byte(query)...)
	req = append(req, packUInt32(uint32(len(index)))...)
	req = append(req, []byte(index)...)
	if hits {
		req = append(req, packUInt32(uint32(1))...)
	} else {
		req = append(req, packUInt32(uint32(0))...)
	}

	var length = len(req)

	log.Println("Build keywords request:", string(req))

	var header bytes.Buffer
	binary.Write(&header, binary.BigEndian, uint16(SEARCHD_COMMAND_KEYWORDS))
	binary.Write(&header, binary.BigEndian, uint16(VER_COMMAND_KEYWORDS))
	binary.Write(&header, binary.BigEndian, uint32(length))

	var request []byte
	request = append(request, header.Bytes()...)
	request = append(request, req...)

	fn := func(err error, response []byte) {
		if err != nil {
			cb(err, nil)
		} else {
			// parse response
			results := []interface{}{}
			p := 0
			rlen := len(response)

			nwords := int(binary.BigEndian.Uint32(response[p : p+4]))
			p = 4
			for i := 0; i < nwords; i++ {
				var len = int(binary.BigEndian.Uint32(response[p : p+4]))
				p += 4
				if p+len > rlen {
					cb(errors.New("Incomplete reply from searchd"), nil)
					return
				}

				tokenized := ""
				if len > 0 {
					tokenized = string(response[p : p+len])
				}
				p += len
				len = int(binary.BigEndian.Uint32(response[p : p+4]))
				p += 4

				normalized := ""
				if len > 0 {
					normalized = string(response[p : p+len])
				}
				p += len

				var entry = map[string]interface{}{
					"tokenized":  tokenized,
					"normalized": normalized,
				}

				if hits {
					entry["docs"] = int(binary.BigEndian.Uint32(response[p : p+4]))
					entry["hits"] = int(binary.BigEndian.Uint32(response[p+4 : p+8]))
					p += 8
				}
				results = append(results, entry)
			}
			cb(nil, results)
		}
	}

	c._sendRequest(VER_COMMAND_KEYWORDS, request, fn)
}

/**
* Get the status
*
* @api public
 */
func (c *SphinxClient) Status(fn func(error, interface{})) {

	var header bytes.Buffer
	binary.Write(&header, binary.BigEndian, uint16(SEARCHD_COMMAND_STATUS))
	binary.Write(&header, binary.BigEndian, uint16(VER_COMMAND_STATUS))
	binary.Write(&header, binary.BigEndian, uint32(4))
	binary.Write(&header, binary.BigEndian, uint32(1))

	c._sendRequest(VER_COMMAND_STATUS, header.Bytes(), func(err error, response []byte) {
		var result = map[string]string{}
		p := 8 // skip 2xINT (NUM_ROWS & NUM COLLS)
		if err == nil {
			for p < len(response) {
				length := int(binary.BigEndian.Uint32(response[p : p+4]))
				p += 4
				key := string(response[p : p+length])
				p += length
				length = int(binary.BigEndian.Uint32(response[p : p+4]))
				p += 4
				value := string(response[p : p+length])
				p += length
				result[key] = value
			}
		}
		fn(err, result)
	})
}

func (c *SphinxClient) Open(fn func(error, interface{})) {

	if (!c._persist) {
		c._client = c.get_connection()

		var header bytes.Buffer
		binary.Write(&header, binary.BigEndian, int16(SEARCHD_COMMAND_PERSIST))
		binary.Write(&header, binary.BigEndian, int16(0))
		binary.Write(&header, binary.BigEndian, uint32(4))
		binary.Write(&header, binary.BigEndian, uint32(1))

		c._persist = true
		_, err := c._client.Write(header.Bytes())
		fn(err, nil)
	} else {
		fn(nil, nil)
	}
}

func (c *SphinxClient) Close() {
	if (c._persist && c._client != nil) {
		c._client.Close()
		c._persist = false
	}
}

func (c *SphinxClient) EscapeString(string) {
	//    return re.sub(r"([=\(\)|\-!@~\"&/\\\^\$\=])", r"\\\1", string)
}

func (c *SphinxClient) FlushAttributes(fn func(error, interface{})) {

	var header bytes.Buffer
	binary.Write(&header, binary.BigEndian, int16(SEARCHD_COMMAND_FLUSHATTRS))
	binary.Write(&header, binary.BigEndian, int16(VER_COMMAND_FLUSHATTRS))
	binary.Write(&header, binary.BigEndian, uint32(0))

	c._sendRequest(VER_COMMAND_FLUSHATTRS, header.Bytes(), func(err error, response []byte) {
		if err != nil {
			fn(err, nil)
		}

		if len(response) != 4 {
			c._error = "unexpected response length"
			fn(err, nil)
		}

		var tag = int(binary.BigEndian.Uint32(response[0:4]))
		fn(err, tag)
	})
}
