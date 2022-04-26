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
package software.amazon.qldb.load;

/**
 * Enumeration to specify data operations when loading revisions.  The ANY value leaves the decision of whether to
 * INSERT, UPDATE, or DELETE to the loader program.  This is useful for times when the data to be loaded should be the
 * current state of the ledger, regardless of what is there already.
 */
public enum Operation {

    INSERT, UPDATE, DELETE, ANY;

    /**
     * @param op String operation value
     * @return The Operation for the given string value or null if no such Operation exists
     */
    public static Operation forString(String op) {
        try {
            return Operation.valueOf(op);
        } catch (Exception e) {
            return null;
        }
    }
}
