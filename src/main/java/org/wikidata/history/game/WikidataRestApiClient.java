package org.wikidata.history.game;

import com.google.common.cache.Cache;
import com.google.common.cache.CacheBuilder;
import okhttp3.*;
import org.eclipse.rdf4j.model.Model;
import org.eclipse.rdf4j.rio.RDFFormat;
import org.eclipse.rdf4j.rio.RDFParseException;
import org.eclipse.rdf4j.rio.Rio;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.io.Reader;
import java.net.SocketTimeoutException;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;

class WikidataRestApiClient {

  private static final Logger LOGGER = LoggerFactory.getLogger(WikidataRestApiClient.class);

  private final OkHttpClient client = new OkHttpClient.Builder()
          .connectTimeout(10, TimeUnit.SECONDS)
          .readTimeout(10, TimeUnit.SECONDS)
          .retryOnConnectionFailure(false)
          .build();
  private final Cache<String, Model> cache = CacheBuilder.newBuilder()
          .maximumSize(10000)
          .expireAfterWrite(10, TimeUnit.MINUTES)
          .build();

  Future<Model> getModelForEntity(String entityId) {
    CompletableFuture<Model> future = new CompletableFuture<>();
    Model cacheModel = cache.getIfPresent(entityId);
    if (cacheModel != null) {
      future.complete(cacheModel);
    } else {
      Request request = new Request.Builder()
              .url("https://www.wikidata.org/wiki/Special:EntityData/" + entityId + ".ttl?flavor=dump")
              .build();
      client.newCall(request).enqueue(new GetModelForEntityCallback(future, cache));
    }
    return future;
  }

  private static final class GetModelForEntityCallback implements Callback {

    private final CompletableFuture<Model> future;
    private final Cache<String, Model> cache;

    private GetModelForEntityCallback(CompletableFuture<Model> future, Cache<String, Model> cache) {
      this.future = future;
      this.cache = cache;
    }

    @Override
    public void onFailure(Call call, IOException e) {
      if (!(e instanceof SocketTimeoutException)) {
        future.cancel(true);
        LOGGER.warn(e.getMessage(), e);
      }
    }

    @Override
    public void onResponse(Call call, Response response) throws IOException {
      try (ResponseBody body = response.body()) {
        if (body == null) {
          future.cancel(true);
          throw new IOException("Unexpected code " + response);
        }
        HttpUrl url = response.request().url();
        String entityId = url.pathSegments().get(url.pathSize() - 1).replace(".ttl", "");
        try (Reader reader = body.charStream()) {
          Model data = Rio.parse(reader, url.toString(), RDFFormat.TURTLE);
          future.complete(data);
          cache.put(entityId, data);
        } catch (RDFParseException e) {
          future.cancel(true);
          throw e;
        }
      }
    }
  }
}
