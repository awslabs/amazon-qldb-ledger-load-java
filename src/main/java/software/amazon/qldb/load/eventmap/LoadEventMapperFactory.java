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

import java.lang.reflect.Method;


/**
 * Builds LoadEventMapper objects.
 */
public class LoadEventMapperFactory {
    /**
     * Builds a LoadEventMapper based on the implementation class name specified in the environment variable LOAD_EVENT_MAPPER.
     * The RevisioLoadEventMappernWriter is built using configuration parameters specified as environment variables which vary by
     * implementation and then initialized.
     *
     * @return The constructed LoadEventMapper
     */
    public static LoadEventMapper buildFromEnvironment() {
        if (!System.getenv().containsKey("LOAD_EVENT_MAPPER")) {
            throw new RuntimeException("LOAD_EVENT_MAPPER property not found in environment");
        }

        LoadEventMapper mapper = null;
        String className = System.getenv("LOAD_EVENT_MAPPER").trim();

        try {
            Class<?> clazz = Class.forName(className);
            Method method = clazz.getMethod("builder");

            LoadEventMapperBuilder builder = (LoadEventMapperBuilder) method.invoke(null);
            builder.configureFromEnvironment();

            mapper = builder.build();
        } catch (Exception e) {
            throw new RuntimeException("Unable to build load event mapper of type \"" + className + "\"", e);
        }

        return mapper;
    }
}
