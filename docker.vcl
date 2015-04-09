sub vcl_recv {
#FASTLY recv

    if (req.request != "HEAD" && req.request != "GET" && req.request != "FASTLYPURGE") {
      return(pass);
    }

   set req.http.X-Filename = regsub(req.url.path, "\..*", "");
   set req.http.X-Token = urldecode(regsub(req.url, ".*token=([^&]+)(?:&|$).*", "\1"));
   set req.http.X-Sig = regsub(req.http.X-Token, "^[^_]+_(.*)", "\1");
   set req.http.X-Exp = regsub(req.http.X-Token, "^([^_]+)_.*", "\1");

   if (time.is_after(now, time.hex_to_time(1,req.http.X-Exp))) {
      error 403;
   }

   if (req.http.X-Sig != digest.hmac_sha1("FASTLY-TOKEN", req.http.X-Filename req.http.X-Exp)) {
      error 403;
   }

   # Strip the token to prevent it making keys all unique
   set req.url = regsub(req.url, "\?.*", "");

   return(lookup);
}

sub vcl_fetch {
#FASTLY fetch

  if ((beresp.status == 500 || beresp.status == 503) && req.restarts < 1 && (req.request == "GET" || req.request == "HEAD")) {
    restart;
  }

  if(req.restarts > 0 ) {
    set beresp.http.Fastly-Restarts = req.restarts;
  }

  if (beresp.http.Set-Cookie) {
    set req.http.Fastly-Cachetype = "SETCOOKIE";
    return (pass);
  }

  if (beresp.http.Cache-Control ~ "private") {
    set req.http.Fastly-Cachetype = "PRIVATE";
    return (pass);
  }

  if (beresp.status == 500 || beresp.status == 503) {
    set req.http.Fastly-Cachetype = "ERROR";
    set beresp.ttl = 1s;
    set beresp.grace = 5s;
    return (deliver);
  }  

  if (beresp.http.Expires || beresp.http.Surrogate-Control ~ "max-age" || beresp.http.Cache-Control ~"(s-maxage|max-age)") {
    # keep the ttl here
  } else {
    # apply the default ttl
    set beresp.ttl = 3600s;
  }

  return(deliver);
}

sub vcl_hit {
#FASTLY hit

  if (!obj.cacheable) {
    return(pass);
  }
  return(deliver);
}

sub vcl_miss {
#FASTLY miss
  return(fetch);
}

sub vcl_deliver {
#FASTLY deliver
  return(deliver);
}

sub vcl_error {
        set obj.http.Content-Type = "text/html; charset=utf-8";

        synthetic {"
        <?xml version="1.0" encoding="utf-8"?>
        <!DOCTYPE html PUBLIC "-//W3C//DTD XHTML 1.0 Strict//EN"
        "http://www.w3.org/TR/xhtml1/DTD/xhtml1-strict.dtd">
        <html>
        <head>
        <title>"} obj.status " " obj.response {"</title>
        </head>
        <body>
        <h1>Error "} req.http.X-Filename  " " now " " time.hex_to_time(1, req.http.X-Exp)  {"</h1>
        <p>"} obj.response {"</p>
        <h3>Guru Meditation:</h3>
        <p>XID: "} req.xid {"</p>
        <address><a href="http://www.varnish-cache.org/">Varnish</a></address>
        </body>
        </html>
        "};
        
        return(deliver);
}

sub vcl_pass {
#FASTLY pass
}
