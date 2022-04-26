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
package software.amazon.qldb.load.util;

import com.amazon.ion.*;
import com.amazon.ion.system.IonSystemBuilder;
import com.amazon.ionhash.IonHashReader;
import com.amazon.ionhash.IonHashReaderBuilder;
import com.amazon.ionhash.MessageDigestIonHasherProvider;
import software.amazon.awssdk.regions.Region;
import software.amazon.awssdk.services.qldbsession.QldbSessionClient;
import software.amazon.awssdk.services.qldbsession.QldbSessionClientBuilder;
import software.amazon.qldb.QldbDriver;
import software.amazon.qldb.Result;
import software.amazon.qldb.RetryPolicy;

import java.util.ArrayList;
import java.util.Base64;
import java.util.Iterator;
import java.util.List;


/**
 * Convenience utilities used in the loader project.
 */
public class LoaderUtils {
    private static final IonSystem ION_SYSTEM = IonSystemBuilder.standard().build();
    private static final MessageDigestIonHasherProvider ION_HASHER_PROVIDER = new MessageDigestIonHasherProvider("SHA-256");


    /**
     * Creates a Base64-encoded SHA-256 hash of the given IonValue.
     *
     * @param value The Ion value to encode
     * @return A Base64-encoded SHA-256 hash of the given Ion value or null if the value argument is null.
     */
    public static String hashIonValue(IonValue value) {
        if (value == null)
            return null;

        IonReader reader = ION_SYSTEM.newReader(value);
        IonHashReader hashReader = IonHashReaderBuilder.standard()
                .withHasherProvider(ION_HASHER_PROVIDER)
                .withReader(reader)
                .build();
        while (hashReader.next() != null) {  }
        return Base64.getEncoder().encodeToString(hashReader.digest());
    }


    /**
     * Instantiates a QLDB driver configured from values specified in environment variables.  The following environment
     * variables are used:
     *
     * <ul>
     *     <li><b>LEDGER_NAME</b>: <i>[REQUIRED]</i> Specifies the name of the ledger to connect to.</li>
     *     <li><b>LEDGER_REGION</b>: Specifies the AWS region the ledger lives in.  Defaults to whatever region the AWS SDK is configured to use as default.</li>
     *     <li><b>MAX_SESSIONS_PER_LAMBDA</b>: Specifies the value of the max concurrent sessions setting of the driver.  Defaults to 1.</li>
     *     <li><b>MAX_OCC_RETRIES</b>: Specifies the number of times the driver will retry transactions on OCC conflicts.  Defaults to 3.</li>
     * </ul>
     *
     * @return An instantiated QLDB driver
     */
    public static QldbDriver createDriverFromEnvironment() {
        if (!System.getenv().containsKey("LEDGER_NAME")) {
            throw new IllegalArgumentException(("Environment not configured with a LEDGER_NAME"));
        }

        String ledgerName = System.getenv("LEDGER_NAME");

        int maxSessions = 1;
        if (System.getenv().containsKey("MAX_SESSIONS_PER_LAMBDA")) {
            try {
                maxSessions = Integer.parseInt(System.getenv("MAX_SESSIONS_PER_LAMBDA"));
            } catch (Exception ignored) {}
        }

        RetryPolicy retryPolicy = RetryPolicy.builder().build();
        if (System.getenv().containsKey("MAX_OCC_RETRIES")) {
            int maxRetries = 3;
            try {
                maxRetries = Integer.parseInt(System.getenv("MAX_OCC_RETRIES"));
            } catch (Exception ignored) {}

            retryPolicy = RetryPolicy.builder().maxRetries(maxRetries).build();
        }

        QldbSessionClientBuilder sessionClientBuilder = QldbSessionClient.builder();
        if (System.getenv().containsKey("LEDGER_REGION")) {
            String str = System.getenv("LEDGER_REGION").trim();
            Region region = Region.of(str);

            if (!Region.regions().contains(region))
                throw new IllegalArgumentException(str + " is not a valid AWS region");

            sessionClientBuilder.region(region);
        }

        return QldbDriver
                .builder()
                .ledger(ledgerName)
                .maxConcurrentTransactions(maxSessions)
                .sessionClientBuilder(sessionClientBuilder)
                .transactionRetryPolicy(retryPolicy)
                .build();
    }


    /**
     * Fetches a list of all active tables in a ledger.
     *
     * @param driver  The driver to use to query the ledger
     * @return A list of all active tables in the ledger or an empty list if there are none
     */
    public static List<String> fetchActiveLedgerTables(QldbDriver driver) {
        return driver.execute(txn -> {
            List<String> actives = new ArrayList<>();

            Result result = txn.execute("select name from information_schema.user_tables where status = 'ACTIVE'");
            Iterator<IonValue> iter = result.iterator();
            while (iter.hasNext()) {
                IonStruct doc = (IonStruct) iter.next();
                actives.add(((IonString) doc.get("name")).stringValue());
            }

            return actives;
        });
    }
}
