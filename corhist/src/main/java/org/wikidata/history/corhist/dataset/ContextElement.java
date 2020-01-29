package org.wikidata.history.corhist.dataset;

import com.fasterxml.jackson.annotation.JsonSubTypes;
import com.fasterxml.jackson.annotation.JsonTypeInfo;

@JsonTypeInfo(use = JsonTypeInfo.Id.NAME, property = "type")
@JsonSubTypes({
        @JsonSubTypes.Type(value = EntityDescription.class, name = "entity"),
        @JsonSubTypes.Type(value = WebPage.class, name = "page"),
})
public interface ContextElement {
}
