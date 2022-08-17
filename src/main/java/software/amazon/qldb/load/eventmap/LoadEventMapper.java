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
import com.amazon.ion.IonValue;

/**
 * Maps elements from an AWS DMS replication event in a Kinesis Data Stream to components need to create
 */
public interface LoadEventMapper {

    /**
     * Maps a table name from the source event to the corresponding table in the target database.
     *
     * @param sourceTable The name of the table in the source database to map.
     * @return The name of the corresponding table in the target database or null if none was found.
     */
    public String mapTableName(String sourceTable);


    /**
     * Maps a data record from a source database into a data record for the target database, handling all field name
     * translation, data formatting and filtering, restructuring, data type conversion, etc.
     *
     * @param sourceRecord The data record from the source database
     * @param beforeImage The "before image" of the record.  This can be useful for updates where the new data record
     *                    may need to consider the before state when setting its values.  This parameter is optional.
     * @param sourceTable The name of the table in the source database that the source record belongs to.
     * @return  A data record mapped to the target table or null if there is no mapping for the given table.  It is
     *          possible for this method to return an empty IonStruct if a table mapping exists, but no fields in the
     *          source data can be mapped.
     */
    public IonStruct mapDataRecord(IonStruct sourceRecord, IonStruct beforeImage, String sourceTable);


    /**
     * Identifies the unique identifier (primary key) in the source record.
     *
     * @param sourceRecord  The data record from the source database
     * @param beforeImage The "before image" of the record.  This can be useful for updates where the value of the unique
     *                    identifier field is changed in the source database.  Without the before image, it would not be
     *                    possible to identify the corresponding record in the target database for update.
     * @param sourceTable The name of the table in the source database that the source record belongs to.
     * @return The value of the unique identifier (primary key) for the record.
     */
    public IonValue mapPrimaryKey(IonStruct sourceRecord, IonStruct beforeImage, String sourceTable);
}
