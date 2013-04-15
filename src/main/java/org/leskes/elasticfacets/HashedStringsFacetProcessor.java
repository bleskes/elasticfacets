package org.leskes.elasticfacets;

import org.elasticsearch.common.component.AbstractComponent;
import org.elasticsearch.common.inject.Inject;
import org.elasticsearch.common.logging.ESLogger;
import org.elasticsearch.common.logging.Loggers;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.trove.set.hash.TIntHashSet;
import org.elasticsearch.common.xcontent.XContentParser;
import org.elasticsearch.index.service.IndexService;
import org.elasticsearch.indices.IndicesService;
import org.elasticsearch.search.facet.Facet;
import org.elasticsearch.search.facet.FacetCollector;
import org.elasticsearch.search.facet.FacetProcessor;
import org.elasticsearch.search.facet.terms.TermsFacet;
import org.elasticsearch.search.internal.SearchContext;
import org.leskes.elasticfacets.fields.HashedStringFieldSettings;
import org.leskes.elasticfacets.fields.HashedStringFieldType;

import java.io.IOException;
import java.util.List;
import java.util.Map;

public class HashedStringsFacetProcessor extends AbstractComponent implements
        FacetProcessor {

   final static ESLogger logger = Loggers
           .getLogger(HashedStringsFacetProcessor.class);
   private final IndicesService indicesService;


   @Inject
   public HashedStringsFacetProcessor(Settings settings, IndicesService indicesService)
   {
      super(settings);
      this.indicesService = indicesService;

      HashedStringsFacet.registerStreams();

   }


   public FacetCollector parse(String facetName, XContentParser parser,
                               SearchContext context) throws IOException {

      String field = null;
      int size = 10;
      int fetch_size = -1;
      boolean allTerms = false;
      String output_scriptLang = null;
      String output_script = null;
      Map<String, Object> params = null;
      HashedStringsFacetCollector.OUTPUT_MODE output_mode = null;




      TIntHashSet excluded = new TIntHashSet();
      TIntHashSet included = new TIntHashSet();
      TermsFacet.ComparatorType comparatorType = TermsFacet.ComparatorType.COUNT;

      String currentFieldName = null;
      XContentParser.Token token;
      while ((token = parser.nextToken()) != XContentParser.Token.END_OBJECT) {
         if (token == XContentParser.Token.FIELD_NAME) {
            currentFieldName = parser.currentName();
         } else if (token == XContentParser.Token.START_ARRAY) {
            if ("exclude".equals(currentFieldName)) {
               while ((token = parser.nextToken()) != XContentParser.Token.END_ARRAY) {
                  if (token == XContentParser.Token.VALUE_NUMBER) {
                     excluded.add(parser.intValue());
                  } else
                     excluded.add(HashedStringFieldType.hashCode(parser.text()));
               }

            }
            else if ("include".equals(currentFieldName)) {
               while ((token = parser.nextToken()) != XContentParser.Token.END_ARRAY) {
                  if (token == XContentParser.Token.VALUE_NUMBER) {
                     included.add(parser.intValue());
                  } else
                     included.add(HashedStringFieldType.hashCode(parser.text()));
               }
            }
         } else if (token.isValue()) {
            if ("field".equals(currentFieldName)) {
               field = parser.text();
            } else if ("size".equals(currentFieldName)) {
               size = parser.intValue();
            } else if ("fetch_size".equals(currentFieldName)) {
               fetch_size = parser.intValue();
            } else if ("all_terms".equals(currentFieldName) || "allTerms".equals(currentFieldName)) {
               allTerms = parser.booleanValue();
            } else if ("order".equals(currentFieldName) || "comparator".equals(currentFieldName)) {
               comparatorType = TermsFacet.ComparatorType.fromString(parser.text());
            } else if ("output_script".equals(currentFieldName)) {
               output_script = parser.text();
            } else if ("output_mode".equals(currentFieldName)) {
               output_mode = HashedStringsFacetCollector.OUTPUT_MODE.fromString(parser.text());
            } else if ("lang".equals(currentFieldName)) {
               output_scriptLang = parser.text();
            } else if (token == XContentParser.Token.START_OBJECT) {
               if ("params".equals(currentFieldName)) {
                  params = parser.map();
               }
            }
         }
      }

      if (fetch_size == -1) fetch_size = size;

      if (output_mode == null) {
         output_mode = (output_script != null) ? HashedStringsFacetCollector.OUTPUT_MODE.SCRIPT :
                 HashedStringsFacetCollector.OUTPUT_MODE.TERM;
      }

      IndexService indexService = indicesService.indexService(context.indexShard().shardId().getIndex());

      HashedStringFieldSettings fieldSettings = indexService.shardInjectorSafe(context.indexShard().shardId().id()).
              getInstance(HashedStringFieldSettings.class);

      HashedStringFieldSettings.FieldTypeFactory fieldTypeFactory = fieldSettings.fieldTypeFactory;

      return new HashedStringsFacetCollector(facetName, field, size, fetch_size, comparatorType, allTerms,
              output_mode, included, excluded, output_script, output_scriptLang, context, params,
              fieldTypeFactory);
   }

   public String[] types() {
      return new String[]{HashedStringsFacet.TYPE};
   }

   @Override
   public Facet reduce(String name, List<Facet> facets) {
      HashedStringsFacet first = (HashedStringsFacet) facets.get(0);
      return first.reduce(name, facets);
   }


}
