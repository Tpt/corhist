package org.wikidata.history.corhist.dataset;

import org.jsoup.Jsoup;
import org.jsoup.nodes.Document;
import org.mapdb.DB;
import org.mapdb.DBMaker;
import org.mapdb.Serializer;

import java.net.URI;
import java.net.URISyntaxException;
import java.net.http.HttpClient;
import java.net.http.HttpRequest;
import java.net.http.HttpResponse;
import java.nio.file.Path;
import java.time.Duration;
import java.util.Map;
import java.util.concurrent.CompletableFuture;

public class WebRetrieval implements AutoCloseable {

  private static final String USER_AGENT = "CorHistBot/0.1 (ttanon@enst.fr)";
  private static final Duration TIMEOUT = Duration.ofSeconds(5);

  private final HttpClient httpClient;
  private final DB db;
  private final Map<String, Integer> pageStatusCodeCache;
  private final Map<String, String> pageContentCache;
  private final Map<String, String> redirectCache;


  public WebRetrieval(Path cachePath) {
    httpClient = HttpClient.newBuilder()
            .followRedirects(HttpClient.Redirect.ALWAYS)
            .connectTimeout(TIMEOUT)
            .build();

    db = DBMaker.fileDB(cachePath.toFile()).fileMmapEnable().cleanerHackEnable().make();
    pageStatusCodeCache = db.hashMap("page-status-code", Serializer.STRING, Serializer.INTEGER).createOrOpen();
    pageContentCache = db.hashMap("page-content", Serializer.STRING, Serializer.STRING).createOrOpen();
    redirectCache = db.hashMap("redirect", Serializer.STRING, Serializer.STRING).createOrOpen();

    Runtime.getRuntime().addShutdownHook(new Thread(db::close));
  }

  @Override
  public void close() {
    db.close();
  }

  public CompletableFuture<WebPage> getWebPage(String url) throws URISyntaxException {
    URI uri = parseURI(url);
    URI location = getFinalLocation(uri);
    Integer pageStatusCode = pageStatusCodeCache.get(location.toString());
    String pageContent = pageContentCache.get(location.toString());
    if (pageStatusCode == null || pageContent == null) {
      //TODO
      //return CompletableFuture.failedFuture(new Exception("skipped"));
      return fetchWebPage(location).thenApply(webPage -> {
        if (!uri.equals(webPage.getLocation())) {
          redirectCache.put(uri.toString(), webPage.getLocation().toString());
        }
        pageStatusCodeCache.put(webPage.getLocation().toString(), webPage.getStatusCode());
        pageContentCache.put(webPage.getLocation().toString(), webPage.getContent());
        return webPage;
      });
    } else {
      return CompletableFuture.completedFuture(new WebPage(location, pageStatusCode, pageContent));
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

  private CompletableFuture<WebPage> fetchWebPage(URI uri) {
    //TODO: follow rel="canonical"
    return httpClient.sendAsync(buildFetchRequest(uri), HttpResponse.BodyHandlers.ofString())
            .thenApply(response -> {
              Document document = Jsoup.parse(response.body(), response.uri().toString()).normalise();
              document.charset();
              return new WebPage(response.uri(), response.statusCode(), document.toString());
            });
  }

  private HttpRequest buildFetchRequest(URI uri) {
    return HttpRequest.newBuilder(uri)
            .GET()
            .setHeader("User-Agent", USER_AGENT)
            .setHeader("Accept", "text/html")
            .timeout(TIMEOUT)
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
