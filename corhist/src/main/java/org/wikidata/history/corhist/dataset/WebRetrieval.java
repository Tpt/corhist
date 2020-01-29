package org.wikidata.history.corhist.dataset;

import okhttp3.*;
import org.jsoup.Jsoup;
import org.jsoup.nodes.Document;
import org.mapdb.DB;
import org.mapdb.DBMaker;
import org.mapdb.Serializer;

import java.io.IOException;
import java.net.URI;
import java.net.URISyntaxException;
import java.nio.file.Path;
import java.util.Arrays;
import java.util.Map;
import java.util.concurrent.TimeUnit;

public class WebRetrieval implements AutoCloseable {

  private static final String USER_AGENT = "CorHistBot/0.1 (ttanon@enst.fr)";

  private final OkHttpClient httpClient;
  private final DB db;
  private final Map<String, Integer> pageStatusCodeCache;
  private final Map<String, String> pageContentCache;
  private final Map<String, String> redirectCache;


  public WebRetrieval(Path cachePath) {
    httpClient = new OkHttpClient.Builder()
            .connectTimeout(1, TimeUnit.SECONDS)
            .readTimeout(5, TimeUnit.SECONDS)
            .writeTimeout(1, TimeUnit.SECONDS)
            .connectionSpecs(Arrays.asList(ConnectionSpec.COMPATIBLE_TLS, ConnectionSpec.CLEARTEXT))
            .retryOnConnectionFailure(false)
            .followRedirects(true)
            .followSslRedirects(true)
            .build();

    db = DBMaker.fileDB(cachePath.toFile()).fileMmapEnable().cleanerHackEnable().checksumHeaderBypass().make();
    pageStatusCodeCache = db.hashMap("page-status-code", Serializer.STRING, Serializer.INTEGER).createOrOpen();
    pageContentCache = db.hashMap("page-content", Serializer.STRING, Serializer.STRING).createOrOpen();
    redirectCache = db.hashMap("redirect", Serializer.STRING, Serializer.STRING).createOrOpen();

    Runtime.getRuntime().addShutdownHook(new Thread(db::close));
  }

  @Override
  public void close() {
    db.close();
  }

  public WebPage getWebPage(String url) throws URISyntaxException, IOException {
    URI uri = parseURI(url);
    URI location = getFinalLocation(uri);
    Integer pageStatusCode = pageStatusCodeCache.get(location.toString());
    String pageContent = pageContentCache.get(location.toString());
    if (pageStatusCode == null || pageContent == null) {
      //TODO
      //return CompletableFuture.failedFuture(new Exception("skipped"));
      WebPage webPage = fetchWebPage(HttpUrl.get(location));
      if (!uri.equals(webPage.getLocation())) {
        redirectCache.put(uri.toString(), webPage.getLocation().toString());
      }
      pageStatusCodeCache.put(webPage.getLocation().toString(), webPage.getStatusCode());
      pageContentCache.put(webPage.getLocation().toString(), webPage.getContent());
      return webPage;
    } else {
      return new WebPage(location, pageStatusCode, pageContent);
    }
  }

  private URI parseURI(String value) throws URISyntaxException {
    URI uri = new URI(value).normalize();
    if (!uri.isAbsolute()) {
      throw new URISyntaxException(uri.toString(), "Relative URIs not supported");
    }
    String scheme = uri.getScheme();
    if (scheme == null || !scheme.equals("http") && !scheme.equals("https")) {
      throw new URISyntaxException(uri.toString(), "Only http and https URIs are supported");
    }
    String path = uri.getPath();
    if (path == null || path.isEmpty()) {
      uri = uri.resolve("/");
    }
    return uri;
  }

  private WebPage fetchWebPage(HttpUrl uri) throws IOException {
    //TODO: follow rel="canonical"
    try (Response response = httpClient.newCall(buildFetchRequest(uri)).execute()) {
      MediaType mediaType = MediaType.get(response.header("Content-Type", "text/html"));
      if (!mediaType.subtype().equalsIgnoreCase("html") && !mediaType.subtype().equalsIgnoreCase("html+xml")) {
        throw new IOException("Unsupported Content-Type: " + response.header("Content-Type"));
      }
      Document document = Jsoup.parse(response.body().string(), response.request().url().toString()).normalise();
      return new WebPage(response.request().url().uri(), response.code(), document.toString());
    } catch (RuntimeException e) {
      throw new IOException(e);
    }
  }

  private Request buildFetchRequest(HttpUrl uri) {
    return new Request.Builder()
            .url(uri)
            .header("User-Agent", USER_AGENT)
            .header("Accept", "text/html")
            .build();
  }

  private URI getFinalLocation(URI uri) {
    String value = redirectCache.get(uri.toString());
    if (value == null) {
      return uri;
    } else {
      try {
        return new URI(value);
      } catch (URISyntaxException e) {
        throw new RuntimeException(e);
      }
    }
  }
}
