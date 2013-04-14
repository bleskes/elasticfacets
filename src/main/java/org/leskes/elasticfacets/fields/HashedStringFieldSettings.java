package org.leskes.elasticfacets.fields;


import org.elasticsearch.common.logging.ESLogger;
import org.elasticsearch.common.logging.Loggers;
import org.elasticsearch.common.settings.Settings;

import java.util.HashMap;
import java.util.Map;

public class HashedStringFieldSettings {
   public static final String HASHED_STRING_PREFIX = "hashed_string.field";
   protected final static ESLogger logger  = Loggers.getLogger(HashedStringFieldSettings.class);

   private static class FieldSettings {
      public int max_terms_per_doc = 0;
      public int min_docs_per_term = 0;
   }

   public static class FieldTypeFactory {
      Map<String,FieldSettings> fieldSettings;

      public FieldTypeFactory(Map<String, FieldSettings> fieldSettings) {
         this.fieldSettings = fieldSettings;
      }

      public HashedStringFieldType getTypeForField(String field) {
         logger.debug("getting type for field {}",field);
         FieldSettings s= fieldSettings.get(field);

         if (s == null) {
            logger.debug("Falling back to default settings for field {}",field);
            s = fieldSettings.get("");
         }
         HashedStringFieldData.HashedStringTypeLoader loader =
                 new HashedStringFieldData.HashedStringTypeLoader(s.max_terms_per_doc,s.min_docs_per_term);
         return new HashedStringFieldType(loader);
      }
   }

   public static FieldTypeFactory processSettings(Settings settings) {
      Map<String, Settings> fieldSettings = settings.getGroups(HASHED_STRING_PREFIX);
      Map<String,FieldSettings> parsedFieldSettings = new HashMap<String, FieldSettings>();
      parsedFieldSettings.put("", new FieldSettings());

      for (Map.Entry<String, Settings> fieldEntry: fieldSettings.entrySet()){
         logger.debug("Settings found for field {}", fieldEntry.getKey());
         FieldSettings s = new FieldSettings();
         s.max_terms_per_doc = fieldEntry.getValue().getAsInt("max_terms_per_doc",0);
         s.min_docs_per_term = fieldEntry.getValue().getAsInt("min_docs_per_term",0);
         parsedFieldSettings.put(fieldEntry.getKey(), s);

      }

      return new FieldTypeFactory(parsedFieldSettings);

   }

}
