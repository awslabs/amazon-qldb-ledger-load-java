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

import com.amazon.ion.IonStruct;
import com.amazon.ion.IonSystem;
import com.amazon.ion.Timestamp;
import com.amazon.ion.system.IonSystemBuilder;
import org.junit.jupiter.api.Test;
import static org.junit.jupiter.api.Assertions.*;

public class MappingFileLoadEventMapperTest {
    private final static IonSystem ION_SYSTEM = IonSystemBuilder.standard().build();

    @Test
    public void testMapTableName() {
        MappingFileLoadEventMapper.MappingFileLoadEventMapperBuilder builder = MappingFileLoadEventMapper.builder();
        builder.setConfigFilename("/dms-mapping-test1.json");
        MappingFileLoadEventMapper mapper = (MappingFileLoadEventMapper) builder.build();
        assertEquals("Person", mapper.mapTableName("person"));
    }

    @Test
    public void testMapPrimaryKey() {
        MappingFileLoadEventMapper.MappingFileLoadEventMapperBuilder builder = MappingFileLoadEventMapper.builder();
        builder.setConfigFilename("/dms-mapping-test1.json");
        MappingFileLoadEventMapper mapper = (MappingFileLoadEventMapper) builder.build();

        IonStruct sourceData = ION_SYSTEM.newEmptyStruct();
        sourceData.put("gov_id").newString("8787");
        sourceData.put("first_name").newString("John");
        sourceData.put("last_name").newString("Doe");

        assertEquals(ION_SYSTEM.newString("8787"), mapper.mapPrimaryKey(sourceData, "person"));
    }

    @Test
    public void testMapDataRecord() {
        MappingFileLoadEventMapper.MappingFileLoadEventMapperBuilder builder = MappingFileLoadEventMapper.builder();
        builder.setConfigFilename("/dms-mapping-test1.json");
        MappingFileLoadEventMapper mapper = (MappingFileLoadEventMapper) builder.build();

        IonStruct sourceData = ION_SYSTEM.newEmptyStruct();
        sourceData.put("gov_id").newString("8787");
        sourceData.put("first_name").newString("John");
        sourceData.put("last_name").newString("Doe");

        IonStruct targetData = mapper.mapDataRecord(sourceData, "person");
        assertNotNull(targetData);
        assertEquals(3, targetData.size());
        assertEquals(ION_SYSTEM.newString("8787"), targetData.get("GovId"));
        assertEquals(ION_SYSTEM.newString("John"), targetData.get("FirstName"));
        assertEquals(ION_SYSTEM.newString("Doe"), targetData.get("LastName"));
    }

    @Test
    public void testMapDataRecordMultipleDataTypes() {
        MappingFileLoadEventMapper.MappingFileLoadEventMapperBuilder builder = MappingFileLoadEventMapper.builder();
        builder.setConfigFilename("/dms-mapping-test2.json");
        MappingFileLoadEventMapper mapper = (MappingFileLoadEventMapper) builder.build();

        IonStruct sourceData = ION_SYSTEM.newEmptyStruct();
        sourceData.put("gov_id").newString("8787");
        sourceData.put("first_name").newString("John");
        sourceData.put("last_name").newString("Doe");
        sourceData.put("age").newInt(30);
        sourceData.put("timestamp").newTimestamp(Timestamp.forDay(2022, 8, 9));

        IonStruct targetData = mapper.mapDataRecord(sourceData, "person");
        assertNotNull(targetData);
        assertEquals(5, targetData.size());
        assertEquals(ION_SYSTEM.newString("8787"), targetData.get("GovId"));
        assertEquals(ION_SYSTEM.newString("John"), targetData.get("FirstName"));
        assertEquals(ION_SYSTEM.newString("Doe"), targetData.get("LastName"));
        assertEquals(ION_SYSTEM.newInt(30), targetData.get("Age"));
        assertEquals(ION_SYSTEM.newTimestamp(Timestamp.forDay(2022, 8, 9)), targetData.get("Timestamp"));
    }


    @Test
    public void testMapDataRecordUnmappedFields() {
        MappingFileLoadEventMapper.MappingFileLoadEventMapperBuilder builder = MappingFileLoadEventMapper.builder();
        builder.setConfigFilename("/dms-mapping-test1.json");
        MappingFileLoadEventMapper mapper = (MappingFileLoadEventMapper) builder.build();

        IonStruct sourceData = ION_SYSTEM.newEmptyStruct();
        sourceData.put("gov_id").newString("8787");
        sourceData.put("first_name").newString("John");
        sourceData.put("last_name").newString("Doe");
        sourceData.put("unmapped1").newString("test1");
        sourceData.put("unmapped2").newString("test2");
        sourceData.put("unmapped3").newString("test3");

        IonStruct targetData = mapper.mapDataRecord(sourceData, "person");
        assertNotNull(targetData);
        assertEquals(3, targetData.size());
        assertEquals(ION_SYSTEM.newString("8787"), targetData.get("GovId"));
        assertEquals(ION_SYSTEM.newString("John"), targetData.get("FirstName"));
        assertEquals(ION_SYSTEM.newString("Doe"), targetData.get("LastName"));
    }

    
    @Test
    public void testMapDataRecordTooFewFields() {
        MappingFileLoadEventMapper.MappingFileLoadEventMapperBuilder builder = MappingFileLoadEventMapper.builder();
        builder.setConfigFilename("/dms-mapping-test1.json");
        MappingFileLoadEventMapper mapper = (MappingFileLoadEventMapper) builder.build();

        IonStruct sourceData = ION_SYSTEM.newEmptyStruct();
        sourceData.put("gov_id").newString("8787");
        sourceData.put("first_name").newString("John");

        IonStruct targetData = mapper.mapDataRecord(sourceData, "person");
        assertNotNull(targetData);
        assertEquals(2, targetData.size());
        assertEquals(ION_SYSTEM.newString("8787"), targetData.get("GovId"));
        assertEquals(ION_SYSTEM.newString("John"), targetData.get("FirstName"));
        assertNull(targetData.get("LastName"));
    }


    @Test
    public void testConfigFileNotFound() {
        MappingFileLoadEventMapper.MappingFileLoadEventMapperBuilder builder = MappingFileLoadEventMapper.builder();
        builder.setConfigFilename("/FILENOTFOUND.json");
        assertThrows(RuntimeException.class, () -> {
            builder.build();
        });
    }


    @Test
    public void testConfigFileBroken() {
        MappingFileLoadEventMapper.MappingFileLoadEventMapperBuilder builder = MappingFileLoadEventMapper.builder();
        builder.setConfigFilename("/dms-mapping-broken.json");
        assertThrows(RuntimeException.class, () -> {
            builder.build();
        });
    }


    @Test
    public void testConfigFileIncomplete() {
        MappingFileLoadEventMapper.MappingFileLoadEventMapperBuilder builder = MappingFileLoadEventMapper.builder();
        builder.setConfigFilename("/dms-mapping-incomplete.json");
        assertThrows(RuntimeException.class, () -> {
            builder.build();
        });
    }
}
