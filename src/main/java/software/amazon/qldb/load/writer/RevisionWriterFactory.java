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
package software.amazon.qldb.load.writer;

import software.amazon.qldb.QldbDriver;
import software.amazon.qldb.load.util.LoaderUtils;

import java.lang.reflect.Method;


/**
 * Builds RevisionWriter objects.
 */
public class RevisionWriterFactory {

    /**
     * Builds a RevisionWriter based on the implementation class name specified in the environment variable REVISION_WRITER.
     * The RevisionWriter is built using configuration parameters specified as environment variables which vary by
     * implementation and then initialized.
     *
     * @return The constructed RevisionWriter
     */
    public static RevisionWriter buildFromEnvironment() {
        if (!System.getenv().containsKey("REVISION_WRITER")) {
            throw new RuntimeException("REVISION_WRITER property not found in environment");
        }

        RevisionWriter writer = null;
        String className = System.getenv("REVISION_WRITER").trim();

        try {
            Class<?> clazz = Class.forName(className);
            Method method = clazz.getMethod("builder");

            RevisionWriterBuilder builder = (RevisionWriterBuilder) method.invoke(null);
            builder.configureFromEnvironment();
            builder.qldbDriver(LoaderUtils.createDriverFromEnvironment());

            writer = builder.build();
        } catch (Exception e) {
            throw new RuntimeException("Unable to build revision writer of type \"" + className + "\"", e);
        }

        return writer;
    }
}
