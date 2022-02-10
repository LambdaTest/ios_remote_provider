package main

import (
    "bufio"
    "crypto/tls"
    "errors"
    "fmt"
    "io/ioutil"
    "net"
    "net/http"
    "net/http/cookiejar"
    "net/url"
    "os"
    "strconv"
    "strings"
    "sync"
    "time"
    "reflect"
    log "github.com/sirupsen/logrus"
    uj "github.com/nanoscopic/ujsonin/v2/mod"
    ws "github.com/gorilla/websocket"
)

type ControlFloor struct {
    config     *Config
    ready      bool
    base       string
    wsBase     string
    cookiejar  *cookiejar.Jar
    client     *http.Client
    root       uj.JNode
    pass       string
    lock       *sync.Mutex
    DevTracker *DeviceTracker
    vidConns   map[string] *ws.Conn
    selfSigned bool
    ToServerCFResponseChan chan *CFResponse //Queue messages here to forward to Control Floor Server
}

func NewControlFloor( config *Config ) (*ControlFloor, chan bool) {
    jar, err := cookiejar.New(&cookiejar.Options{})
    if err != nil {
        panic( err )
    }
    
    root := loadCFConfig( "cf.json" )
    passNode := root.Get("pass")
    if passNode == nil {
    }
    
    pass := passNode.String()
    
    client := &http.Client{
        Jar: jar,
        CheckRedirect: func(req *http.Request, via []*http.Request) error {
            return http.ErrUseLastResponse
        },
    }
    
    self := ControlFloor{
        config: config,
        ready: false,
        base: "http://" + config.cfHost,
        wsBase: "ws://" + config.cfHost,
        cookiejar: jar,
        client: client,
        pass: pass,
        lock: &sync.Mutex{},
        ToServerCFResponseChan: make( chan *CFResponse, 100 ),
        vidConns: make( map[string] *ws.Conn ),
    }
    if config.https {
        self.base = "https://" + config.cfHost
        self.wsBase = "wss://" + config.cfHost
        if config.selfSigned {
            self.selfSigned = true
            tr := &http.Transport{
                TLSClientConfig: &tls.Config{InsecureSkipVerify: true},
                ForceAttemptHTTP2: false,
            }
            client.Transport = tr
        }
    } else {
        self.base = "http://" + config.cfHost
        self.wsBase = "ws://" + config.cfHost
    }
    
    stopCf := make( chan bool )
    go func() {
        exit := false
        log.Info("ControlFloor: new thread. Server auto-reconnect1")
        LOOP:
        for {
            select {
              case <- stopCf:
                exit = true
                break LOOP
              default:
            }
            if exit { break }
            
            success := self.login()
            if success {
                log.WithFields( log.Fields{
                    "type": "cf_login_success",
                } ).Info( "Logged in to control floor" )
            } else {
                fmt.Println("Could not login to control floor")
                fmt.Println("Waiting 10 seconds to retry...")
                time.Sleep( time.Second * 10 )
                fmt.Println("trying again\n")
                continue
            }
            
            self.openWebsocket()
            log.Info("ControlFloor: new thread. Server auto-reconnect2")
            
        }
    }()
    
    return &self, stopCf
}
// Called from the device object
func ( self *ControlFloor ) connectVidChannel( udid string ) *ws.Conn {
    dialer := ws.Dialer{
        Jar: self.cookiejar,
    }
    
    if self.selfSigned {
        fmt.Printf("self signed option\n")
        dialer.TLSClientConfig = &tls.Config{InsecureSkipVerify: true}
        //ws.DefaultDialer.TLSClientConfig = &tls.Config{InsecureSkipVerify: true}
    }
    
    fmt.Printf("Connecting to CF imgStream\n")
    var conn *ws.Conn
    var resp *http.Response
    for i:=0; i<5; i++ {
        var err error
        conn, resp, err = dialer.Dial( self.wsBase + "/provider/imgStream?udid=" + udid, nil )
        if err != nil {
            fmt.Printf( "Error dialing:%s\n", err )
            fmt.Printf( "Status code: %d", resp.StatusCode )
            resp.Body.Close()
            bytes, err := ioutil.ReadAll( resp.Body )
            if err == nil && len( bytes ) > 0 {
                fmt.Printf("Body: %s\n", string(bytes) )
            }
            time.Sleep( time.Millisecond * 100 )
            continue
        }
        break
    }
    
    fmt.Printf("Connected CF imgStream\n")
    
    //dev := self.DevTracker.getDevice( udid )
    
    self.lock.Lock()
    self.vidConns[ udid ] = conn
    self.lock.Unlock()
    
    return conn
    //dev.startStream( conn )
}

// Called from the device object
func ( self *ControlFloor ) destroyVidChannel( udid string ) {
    vidConn, exists := self.vidConns[ udid ]
    
    if !exists { return }
    
    self.lock.Lock()
    delete( self.vidConns, udid )
    self.lock.Unlock()
    
    vidConn.Close()
}

func ( self *ControlFloor ) openWebsocket() {
    dialer := ws.Dialer{
        Jar: self.cookiejar,
    }
    
    if self.selfSigned {
        log.WithFields( log.Fields{
            "type": "cf_ws_selfsign",
        } ).Warn( "ControlFloor connection is self signed" )
        dialer.TLSClientConfig = &tls.Config{InsecureSkipVerify: true}
        //ws.DefaultDialer.TLSClientConfig = &tls.Config{InsecureSkipVerify: true}
    }
    
    log.WithFields( log.Fields{
        "type": "cf_ws_connect",
        "link": ( self.wsBase + "/provider/ws" ),
    } ).Info( "Connecting ControlFloor WebSocket" )
    
    conn, _, err := dialer.Dial( self.wsBase + "/provider/ws", nil )
    if err != nil {
        panic( err )
    }
    
    //respondChan := make( chan CFResponseXX )
    doneChan := make( chan bool )
    // response channel exists so that multiple threads can queue
    //   responses. WriteMessage is not thread safe
    go func() { 
        log.Info("ControlFloor: new thread. ToServerCFResponseChan -> send to server")
        LOOP:
        for { //TODO: indent
        select {
            case <- doneChan:
                fmt.Println("Control Floor -> server, received message to terminate....")
                break LOOP
            case cfresponse := <- self.ToServerCFResponseChan:
                bytes, _ := cfresponse.JSONBytes()
                err := conn.WriteMessage( ws.TextMessage, bytes )
                if err != nil {
                    fmt.Printf("Error writing to server ws\n")
                    break LOOP
                }else{
                    fmt.Printf( "Wrote response back: %s\n", string(bytes) )
                }
        }
    } 
    log.Info("ControlFloor: terminated thread. ToServerCFResponseChan -> send to server")

    }()
    self.DevTracker.cfReady()
    
    /*go func() { for {
        err := conn.WriteMessage( ws.TextMessage, []byte("{id:0,type:'ping'}") )
        if err != nil {
            fmt.Printf("Lost ws connection to ControlFloor\n")
        }
        time.Sleep( time.Second )
    } }()*/
        
    // There is only a single websocket connection between a provider and controlfloor
    // As a result, all messages sent here ought to be small, because if they aren't
    // other messages will be delayed being received and some action started.
//    go func(){ //TODO: re-indent
//    log.Info("ControlFloor: new thread. From Server -> device[udid].CFRequestChan")
//NOTE:  The current thread is the auto-reconnect thread
    for {
        t, msg, err := conn.ReadMessage()
        if err != nil {
            fmt.Printf("Error reading from ws\n")
            break
        }
        if t != ws.TextMessage {
           fmt.Printf("Received non-text message, type: %v",t)
        }

        cfrequest, err := NewInternalCFRequestFromJSON(msg) 
        if err != nil {
            fmt.Printf("Error decoding cfrequest1.\n %v\n%s\n\n",err,msg)
            continue
        }
        deviceID := cfrequest.CFDeviceID
        action   := cfrequest.Action
        if cfrequest.Action == "ping" && cfrequest.CFDeviceID==""{
            self.ToServerCFResponseChan <- NewPongResponse(cfrequest)
            fmt.Printf("Reply to server ping: pong\n")
        }else if(cfrequest.Action == ""){
            fmt.Printf("Dropping message with no action specified %s\n",msg)
            continue //TODO: error reply
        }else if action == "shutdown" {
            self.ToServerCFResponseChan <- NewOkResponse(cfrequest)
            do_shutdown( self.config, self.DevTracker )
        }else{
            //TODO: make controlfloor single-device?
            dev := self.DevTracker.getDevice( deviceID )
            dev.CFRequestChan <- cfrequest
        }
    }
//    log.Info("ControlFloor: terminated thread. From Server -> device[udid].CFRequestChan")
//    }()
    doneChan <- true
}

func loadCFConfig( configPath string ) (uj.JNode) {
    fh, serr := os.Stat( configPath )
    if serr != nil {
        log.WithFields( log.Fields{
            "type":        "err_read_config",
            "error":       serr,
            "config_path": configPath,
        } ).Fatal(
            "Could not read ControlFloor auth token. Have you run `./main register`?",
        )
    }
    configFile := configPath
    switch mode := fh.Mode(); {
        case mode.IsDir(): configFile = fmt.Sprintf("%s/config.json", configPath)
    }
    content, err := ioutil.ReadFile( configFile )
    if err != nil { log.Fatal( err ) }
	
    root, _, perr := uj.ParseFull( content )
    if perr != nil {
        log.WithFields( log.Fields{
            "error": perr,
        } ).Fatal(
            "ControlFloor auth token is invalid. Rerun `./main register`",
        )
    }
    
    return root
}

func writeCFConfig( configPath string, pass string ) {
    bytes := []byte(fmt.Sprintf("{pass:\"%s\"}\n",pass))
    err := ioutil.WriteFile( configPath, bytes, 0644)
    if err != nil {
        panic( err )
    }
}

func (self *ControlFloor) baseNotify( name string, udid string, variant string, vals url.Values ) {
    ok := self.checkLogin()
    if ok == false {
        panic("Could not login when attempting '" + name + "' notify")
    }
    
    resp, err := self.client.PostForm( self.base + "/provider/device/status/" + variant, vals )
    if err != nil {
        panic( err )
    }
    
    // Ensure the request is closed out
    defer resp.Body.Close()
    ioutil.ReadAll(resp.Body)
    
    if resp.StatusCode != 200 {
        log.WithFields( log.Fields{
            "type": "cf_notify_fail",
            "variant": variant,
            "udid": censorUuid( udid ),
            "values": vals,
            "httpStatus": resp.StatusCode,
        } ).Error( fmt.Sprintf("Failure notifying CF of %s", name) )
    } else {
        log.WithFields( log.Fields{
            "type": "cf_notify",
            "name": name,
            "udid": censorUuid( udid ),
        } ).Info( fmt.Sprintf("Notifying CF of %s", name) )
    }
}

func (self *ControlFloor) orientationChange( udid string, orientation string ) {
      log.WithFields( log.Fields{
         "type": "cf_notify",
         "udid": censorUuid( udid ),
      } ).Info( fmt.Sprintf("Notifying CF of Orientation Change %s", orientation) )
     
      self.ToServerCFResponseChan <- NewOrientationChangedNotice(udid,orientation)
//    ok := self.checkLogin()
//    if ok == false {
//        panic("Could not login when notifying of orientation change to '" + orientation + "' notify")
//    }
//    
//    resp, _ := self.client.PostForm( self.base + "/provider/device/orientation", url.Values{
//        "udid": {udid},
//        "orientation": {orientation},
//    } )
//    
//    // Ensure the request is closed out
//    defer resp.Body.Close()
//    ioutil.ReadAll(resp.Body)
}

func productTypeToCleanName( prodType string ) string {
    if strings.HasPrefix( prodType, "iPhone" ) {
        prodType = prodType[6:]
        typeToName := map[string]string {
            "1,1": "",            "1,2": "3G",         "2,1": "3GS",      "3,1": "4",
            "3,2": "4",           "3,3": "4",          "4,1": "4S",       "4,2": "4S",
            "4,3": "4S",          "5,1": "5",          "5,2": "5",        "5,3": "5C",
            "5,4": "5C",          "6,1": "5S",         "6,2": "5S",       "7,2": "6",
            "7,1": "6 Plus",      "8,1": "6S",         "8,2": "6S Plus",  "8,4": "SE",
            "9,1": "7",           "9,3": "7",          "9,2": "7 Plus",   "9,4": "7 Plus",
            "10,1": "8",          "10,4": "8",         "10,2": "8 Plus",  "10,5": "8 Plus",
            "10,3": "X",          "10,6": "X",         "11,2": "Xs",      "11,4": "Xs Max",
            "11,6": "Xs Max",     "11,8": "Xʀ",        "12,1": "11",      "12,3": "11 Pro",
            "12,5": "11 Pro Max", "12,8": "SE 2",      "13,1": "12 mini", "13,2": "12",
            "13,3": "12 Pro",     "13,4": "12 Pro Max","14,2": "13 pro",  "14,3": "13 Prox Max",
            "14,4": "13 mini",    "14,5": "13",
        }
        name, exists := typeToName[ prodType ]
        if exists { return "iPhone " + name }
        return prodType
    }
    if strings.HasPrefix( prodType, "iPad" ) {
        prodType = prodType[4:]
        typeToName := map[string]string {
            "1,1": "",              "2,1": "2",             "2,2": "2",             "2,3": "2",
            "2,4": "2",             "2,5": "Mini",          "2,6": "Mini",          "2,7": "Mini", 
            "3,1": "3",             "3,2": "3",             "3,3": "3",             "3,4": "4",
            "3,5": "4",             "3,6": "4",             "4,1": "Air",           "4,2": "Air",
            "4,3": "Air",           "4,4": "Mini 2",        "4,5": "Mini 2",        "4,6": "Mini 2",
            "4,7": "Mini 3",        "4,8": "Mini 3",        "4,9": "Mini 3",        "5,1": "Mini 4",    
            "5,2": "Mini 4",        "5,3": "Air 2",         "5,4": "Air 2",         "6,3": "Pro 9.7in",
            "6,4": "Pro 9.7in",     "6,7": "Pro 12.9in",    "6,8": "Pro 12.9in",    "6,11": "5",
            "6,12": "5",            "7,1": "Pro 12.9in 2",  "7,2": "Pro 12.9in 2",  "7,3": "Pro 10.5in",
            "7,4": "Pro 10.5in",    "7,5": "6",             "7,6": "6",             "7,11": "7",    
            "7,12": "7",            "8,1": "Pro 11in",      "8,2": "Pro 11in",      "8,3": "Pro 11in",
            "8,4": "Pro 11in",      "8,5": "Pro 12.9in 3",  "8,6": "Pro 12.9in 3",  "8,7": "Pro 12.9in 3",
            "8,8": "Pro 12.9in 3",  "8,9": "Pro 11in 2",    "8,10": "Pro 11in 2",   "8,11": "Pro 12.9in 4",
            "8,12": "Pro 12.9in 4", "11,1": "Mini 5",       "11,2": "Mini 5",       "11,3": "Air 3",
            "11,4": "Air 3",        "11,6": "8",            "11,7": "8",            "12,1": "8",
            "12,2": "8",            "13,1": "Air 4",        "13,2": "Air 4",        "13,4": "Pro 11in 3",
            "13,5": "Pro 11in 3",   "13,6": "Pro 11in 3",   "13,7": "Pro 11in 3",   "13,8": "Pro 12.9in 5",
            "13,9": "Pro 12.9in 5", "13,10": "Pro 12.9in 5","13,11": "Pro 12.9in 5","14,1": "Mini 6",
            "14,2": "Mini 6",
        }
        name, exists := typeToName[ prodType ]
        if exists { return "iPhone " + name }
        return prodType
    }
    return prodType
}

func (self *ControlFloor) notifyDeviceInfo( dev *Device, artworkTraits uj.JNode ) {
    info := dev.info
    udid := dev.udid
    str := "{"
    for key, val := range info {
        str = str + fmt.Sprintf("\"%s\":\"%s\",", key, val )
    }
    
    prodDescr := "unknown"
    if artworkTraits != nil {
        prodDescr = artworkTraits.Get("ArtworkDeviceProductDescription").String()
    } else {
        prodDescr = productTypeToCleanName( info[ "ProductType" ] )
    }
    str = str + "\"ArtworkDeviceProductDescription\":\"" + prodDescr + "\"\n"
    str = str + "}"
    
    self.baseNotify("device info", udid, "info", url.Values{
        "udid": {udid},
        "info": {str},
    } )
}

func (self *ControlFloor) notifyDeviceExists( udid string, width int, height int, clickWidth int, clickHeight int ) {
    self.baseNotify("device existence", udid, "exists", url.Values{
        "udid": {udid},
        "width": {strconv.Itoa(width)},
        "height": {strconv.Itoa(height)},
        "clickWidth": {strconv.Itoa(clickWidth)},
        "clickHeight": {strconv.Itoa(clickHeight)},
    } )
}

func (self *ControlFloor) notifyProvisionStopped( udid string ) {
    self.baseNotify("provision stop", udid, "provisionStopped", url.Values{
        "udid": {udid},
    } )
}

func (self *ControlFloor) notifyWdaStopped( udid string ) {
    self.baseNotify("WDA stop", udid, "wdaStopped", url.Values{
        "udid": {udid},
    } )
}

func (self *ControlFloor) notifyWdaStarted( udid string, port int ) {
    self.baseNotify("WDA start", udid, "wdaStarted", url.Values{
        "udid": {udid},
        "port": {strconv.Itoa(port)},
    } )
}

func (self *ControlFloor) notifyCfaStopped( udid string ) {
    self.baseNotify("CFA stop", udid, "cfaStopped", url.Values{
        "udid": {udid},
    } )
}

func (self *ControlFloor) notifyCfaStarted( udid string ) {
    self.baseNotify("CFA start", udid, "cfaStarted", url.Values{
        "udid": {udid},
    } )
}

func (self *ControlFloor) notifyVideoStopped( udid string ) {
    self.baseNotify("video stop", udid, "videoStopped", url.Values{
        "udid": {udid},
    } )
}

func (self *ControlFloor) notifyVideoStarted( udid string ) {
    self.baseNotify("video start", udid, "videoStarted", url.Values{
        "udid": {udid},
    } )
}

func (self *ControlFloor) checkLogin() (bool) {
    self.lock.Lock()
    ready := self.ready
    self.lock.Unlock()
    if ready { return true }
    return self.login()
}

func (self *ControlFloor) login() (bool) {
    self.lock.Lock()
    
    user := self.config.cfUsername
    pass := self.pass
    
    resp, err := self.client.PostForm( self.base + "/provider/login",
        url.Values{
            "user": {user},
            "pass": {pass},
        },
    )
    if err != nil {
        var urlError *url.Error
        if errors.As( err, &urlError ) {
            var netOpError *net.OpError 
            if errors.As( urlError, &netOpError ) {
                rootErr := netOpError.Err
                if( rootErr.Error() == "connect: connection refused" ) {
                    fmt.Printf("Could not connect to ControlFarm; is it running?\n")
                } else {
                    fmt.Printf("Err type:%s - %s\n", reflect.TypeOf(err), err )
                    fmt.Printf("urlError type:%s - %s\n", reflect.TypeOf(urlError), urlError );
                    fmt.Printf("netOpError type:%s - %s\n", reflect.TypeOf(netOpError), netOpError )
                }
            } else {
                fmt.Printf("Err type:%s - %s\n", reflect.TypeOf(err), err )
                fmt.Printf("urlError type:%s - %s\n", reflect.TypeOf(urlError), urlError );
            }
        } else {
            fmt.Printf("Err type:%s - %s\n", reflect.TypeOf(err), err )
        }
        self.lock.Unlock()
        return false
        //panic( err )
    }
    
    // Ensure the request is closed out
    defer resp.Body.Close()
    ioutil.ReadAll(resp.Body)
    
    success := false
    if resp.StatusCode != 302 {
        success = false
        fmt.Printf("StatusCode from controlfloor login:'%d'\n", resp.StatusCode )
    } else {
        loc, _ := resp.Location()
        
        q := loc.RawQuery
        if q != "fail=1" {
            success = true
        } else {
            fmt.Printf("Location from redirect of controlfloor login:'%s'\n", loc )
        }
    }
    
    if !success {
        self.ready = false
        self.lock.Unlock()
        return false
    }
    self.ready = true
    self.lock.Unlock()
    return true
}

func doregister( config *Config ) (string) {
    // query cli for registration password
    reader := bufio.NewReader( os.Stdin )
    fmt.Print("Enter registration password:")
    regPass, _ := reader.ReadString('\n')
    if regPass == "\n" {
        regPass = "doreg"
        fmt.Printf("Using default registration password of %s\n", regPass)
    }
    
    username := config.cfUsername
    // send registration to control floor with id and public key
    protocol := "http"
    if config.https {
        protocol = "https"
    }
    resp, err := http.PostForm( protocol + "://" + config.cfHost + "/provider/register",
        url.Values{
            "regPass": {regPass},
            "username": {username},
        },
    )
    if err != nil {
        panic( err )
    }
    if resp.Body == nil {
        panic("registration respond body is empty")
    }
    defer resp.Body.Close()
    
    body, readErr := ioutil.ReadAll( resp.Body )
    if readErr != nil {
        panic( readErr )
    }
    
    //fmt.Println( string(body) )
    root, _ := uj.Parse( body )
    
    sNode := root.Get("Success")
    if sNode == nil {
        panic( "No Success node in registration result" )
    }
    success := sNode.Bool()
    if !success {
        panic("Registration failed")
    }
    
    existed := root.Get("Existed").Bool()
    pass := root.Get("Password").String()
    fmt.Printf("Registered and got password %s\n", pass)
    if existed {
        fmt.Printf("User %s existed so password was renewed\n", username )
    }
    
    writeCFConfig( "cf.json", pass )
            
    return pass
}
