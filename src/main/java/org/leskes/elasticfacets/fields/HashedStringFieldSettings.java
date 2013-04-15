package org.leskes.elasticfacets.fields;


import org.elasticsearch.cluster.settings.DynamicSettings;
import org.elasticsearch.common.inject.Inject;
import org.elasticsearch.common.logging.ESLogger;
import org.elasticsearch.common.logging.Loggers;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.trove.set.hash.TIntHashSet;
import org.elasticsearch.index.settings.IndexDynamicSettings;
import org.elasticsearch.index.settings.IndexSettings;
import org.elasticsearch.index.settings.IndexSettingsService;
import org.elasticsearch.index.shard.AbstractIndexShardComponent;
import org.elasticsearch.index.shard.ShardId;

import java.util.HashMap;
import java.util.Map;
import java.util.regex.Pattern;

public class HashedStringFieldSettings extends AbstractIndexShardComponent {
   public static final String HASHED_STRINGS_FIELD = "index.hashed_strings.field";
   protected final static ESLogger logger  = Loggers.getLogger(HashedStringFieldSettings.class);

   @Inject
   protected HashedStringFieldSettings(ShardId shardId, @IndexSettings Settings indexSettings,
                                       IndexSettingsService indexSettingsService,
                                       @IndexDynamicSettings DynamicSettings dynamicSettings) {
      super(shardId, indexSettings);
      dynamicSettings.addDynamicSettings(HASHED_STRINGS_FIELD + ".*");
      fieldTypeFactory = processSettings(indexSettings);
      indexSettingsService.addListener(applySettings);

   }

   public FieldTypeFactory fieldTypeFactory = null;

   class ApplySettings implements IndexSettingsService.Listener{
      @Override
      public void onRefreshSettings(Settings settings) {
         HashedStringFieldSettings.this.fieldTypeFactory = HashedStringFieldSettings.this.processSettings(settings);
      }
   }

   private final ApplySettings applySettings = new ApplySettings();



   private static class FieldSettings {
      public int max_terms_per_doc = 0;
      public int min_docs_per_term = 0;
      public TIntHashSet excludeTerms = null;
      public Pattern excludePattern = null;

      @Override
      public String toString() {
         return String.format("{ max_terms_per_doc: %s, min_docs_per_term: %s, excludeTerms #: %s, excludePattern: %s} ",
                 max_terms_per_doc, min_docs_per_term, excludeTerms == null? 0: excludeTerms.size(),
                 excludePattern == null ? "" : excludePattern.pattern());
      }
   }

   public static class FieldTypeFactory {
      Map<String,FieldSettings> fieldSettings;

      public FieldTypeFactory(Map<String, FieldSettings> fieldSettings) {
         this.fieldSettings = fieldSettings;
      }

      public HashedStringFieldType getTypeForField(String field) {
         logger.trace("getting type for field {}",field);
         FieldSettings s= fieldSettings.get(field);

         if (s == null) {
            logger.trace("Falling back to default settings for field {}",field);
            s = fieldSettings.get("");
         }
         HashedStringFieldData.HashedStringTypeLoader loader =
                 new HashedStringFieldData.HashedStringTypeLoader(
                         s.max_terms_per_doc,s.min_docs_per_term, s.excludePattern, s.excludeTerms);
         return new HashedStringFieldType(loader);
      }
   }

   protected FieldTypeFactory processSettings(Settings settings) {
      Map<String, Settings> fieldSettings = settings.getGroups(HASHED_STRINGS_FIELD);
      Map<String,FieldSettings> parsedFieldSettings = new HashMap<String, FieldSettings>();
      parsedFieldSettings.put("", new FieldSettings());

      for (Map.Entry<String, Settings> fieldEntry: fieldSettings.entrySet()){
         logger.debug("Settings found for field {}", fieldEntry.getKey());
         FieldSettings s = new FieldSettings();
         s.max_terms_per_doc = fieldEntry.getValue().getAsInt("max_terms_per_doc",0);
         s.min_docs_per_term = fieldEntry.getValue().getAsInt("min_docs_per_term",0);
         String[] excludeTerms = fieldEntry.getValue().getAsArray("exclude",new String[] {});
         if (excludeTerms.length != 0) {
            TIntHashSet excludeSet = new TIntHashSet(excludeTerms.length);
            for (String t: excludeTerms)
               excludeSet.add(HashedStringFieldType.hashCode(t));
            s.excludeTerms = excludeSet;
         }

         String excludePattern = fieldEntry.getValue().get("exclude_regex");
         if (excludePattern!=null && !excludePattern.isEmpty()) {
            s.excludePattern = Pattern.compile(excludePattern, Pattern.CASE_INSENSITIVE | Pattern.UNICODE_CASE);
         }

         logger.info("Loaded custom settings for {}: {}", fieldEntry.getKey(), s );

         parsedFieldSettings.put(fieldEntry.getKey(), s);

      }

      return new FieldTypeFactory(parsedFieldSettings);

   }

}
