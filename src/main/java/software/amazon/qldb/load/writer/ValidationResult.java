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


/**
 * Represents the result of a validation operation in a {@link software.amazon.qldb.load.writer.RevisionWriter}.
 */
public class ValidationResult {
    public boolean skip = false;
    public boolean fail = false;
    public String message = null;


    /**
     * @return a skip result with no message
     */
    public static ValidationResult skip() {
        ValidationResult result = new ValidationResult();
        result.skip = true;
        return result;
    }

    /**
     * @param message The message to include in the skip result
     * @return a skip result with the given message
     */
    public static ValidationResult skip(String message) {
        ValidationResult result = new ValidationResult();
        result.skip = true;
        result.message = message;
        return result;
    }

    /**
     * @param message The message to include in the failure result
     * @return a failure result with the given message
     */
    public static ValidationResult fail(String message) {
        ValidationResult result = new ValidationResult();
        result.fail = true;
        result.message = message;
        return result;
    }

    /**
     * @return A pass (success) result.
     */
    public static ValidationResult pass() {
        return new ValidationResult();
    }
}

