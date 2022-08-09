/*
 * Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
 * SPDX-License-Identifier: MIT-0
 *
 * Permission is hereby granted, free of charge, to any person obtaining a copy of this
 * software and associated documentation files (the "Software"), to deal in the Software
 * without restriction, including without limitation the rights to use, copy, modify,
 * merge, publish, distribute, sublicense, and/or sell copies of the Software, and to
 * permit persons to whom the Software is furnished to do so.
 *
 * THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND, EXPRESS OR IMPLIED,
 * INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF MERCHANTABILITY, FITNESS FOR A
 * PARTICULAR PURPOSE AND NONINFRINGEMENT. IN NO EVENT SHALL THE AUTHORS OR COPYRIGHT
 * HOLDERS BE LIABLE FOR ANY CLAIM, DAMAGES OR OTHER LIABILITY, WHETHER IN AN ACTION
 * OF CONTRACT, TORT OR OTHERWISE, ARISING FROM, OUT OF OR IN CONNECTION WITH THE
 * SOFTWARE OR THE USE OR OTHER DEALINGS IN THE SOFTWARE.
 */
package software.amazon.qldb.load.eventmap;

import com.amazon.ion.*;
import com.amazon.ion.system.IonSystemBuilder;

import java.io.InputStream;
import java.util.HashMap;
import java.util.Iterator;
import java.util.Map;


/**
 * Maps DMS load events to QLDB document revisions based on a JSON file that defines the mapping of tables and fields
 * between the source and target databases.
 */
public class MappingFileLoadEventMapper implements LoadEventMapper {
    private final static IonSystem ION_SYSTEM = IonSystemBuilder.standard().build();
    private final Map<String, TableConfig> tableMap = new HashMap<>();

    private String configFilename = "/dms-mapping.json";

    public MappingFileLoadEventMapper(String configFilename) {
        if (configFilename != null)
            this.configFilename = configFilename;

        loadMappingFile();
    }


    @Override
    public String mapTableName(String sourceTable) {
        TableConfig config = tableMap.get(sourceTable);

        if (config == null)
            return null;

        return config.targetTable;
    }

    @Override
    public IonStruct mapDataRecord(IonStruct sourceRecord, String sourceTable) {
        TableConfig config = tableMap.get(sourceTable);
        if (config == null)
            return null;

        IonStruct data = ION_SYSTEM.newEmptyStruct();
        Iterator<IonValue> iter = sourceRecord.iterator();
        while (iter.hasNext()) {
            IonValue field = iter.next();
            if (config.fieldMap.containsKey(field.getFieldName())) {
                data.put(config.fieldMap.get(field.getFieldName()), field.clone());
            }
        }

        return data;
    }

    @Override
    public IonValue mapPrimaryKey(IonStruct sourceRecord, String sourceTable) {
        TableConfig config = tableMap.get(sourceTable);
        if (config == null)
            return null;

        return sourceRecord.get(config.primaryKeyField);
    }


    public static MappingFileLoadEventMapperBuilder builder() {
        return new MappingFileLoadEventMapperBuilder();
    }


    private void loadMappingFile() {
        try (InputStream in = this.getClass().getResourceAsStream(configFilename)) {
            IonDatagram datagram = ION_SYSTEM.getLoader().load(ION_SYSTEM.newReader(in));
            for (IonValue gramValue : datagram) {
                if (gramValue.getType() != IonType.LIST) {
                    continue;
                }

                for (IonValue val : (IonList) gramValue) {
                    if (!(val.getType() == IonType.STRUCT)) {
                        continue;
                    }

                    IonStruct item = (IonStruct) val;
                    TableConfig config = new TableConfig();

                    config.primaryKeyField = ((IonString) item.get("id-field")).stringValue();
                    config.sourceTable = ((IonString) item.get("source-table")).stringValue();
                    config.targetTable = ((IonString) item.get("target-table")).stringValue();

                    IonList fields = (IonList) item.get("fields");
                    for (int i = 0; i < fields.size(); i++) {
                        IonStruct field = (IonStruct) fields.get(i);
                        String sourceField = ((IonString) field.get("source-field")).stringValue();
                        String targetField = ((IonString) field.get("target-field")).stringValue();
                        config.fieldMap.put(sourceField, targetField);
                    }

                    tableMap.put(config.sourceTable, config);
                }
            }
        } catch (Exception e) {
            throw new RuntimeException("Error loading config file", e);
        }
    }


    private static class TableConfig {
        public String sourceTable;
        public String targetTable;
        public String primaryKeyField;
        public Map<String, String> fieldMap = new HashMap<>();
    }


    public static class MappingFileLoadEventMapperBuilder implements LoadEventMapperBuilder {
        private String configFilename = null;

        public void setConfigFilename(String configFilename) {
            this.configFilename = configFilename;
        }

        @Override
        public LoadEventMapper build() {
            return new MappingFileLoadEventMapper(configFilename);
        }

        @Override
        public LoadEventMapperBuilder configureFromEnvironment() {
            return this;
        }
    }
}
