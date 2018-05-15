/*
 * Made with all the love in the world
 * by scireum in Remshalden, Germany
 *
 * Copyright by scireum GmbH
 * http://www.scireum.de - info@scireum.de
 */

package sirius.db.mongo;

import org.bson.Document;
import sirius.kernel.commons.Tuple;
import sirius.kernel.commons.Values;
import sirius.kernel.di.std.Part;
import sirius.kernel.di.std.Register;
import sirius.kernel.health.console.Command;

import javax.annotation.Nonnull;
import java.util.Map;

/**
 * Reports statistics and controls tracing via the system console.
 */
@Register
public class MongoCommand implements Command {

    @Part
    private Mongo mongo;

    @Override
    public void execute(Output output, String... params) throws Exception {
        if (!mongo.isConfigured()) {
            output.line("Mongo DB is not configured.");
            return;
        }

        Values parameters = Values.of(params);
        if ("tracing".equals(parameters.at(0).asString())) {
            if (parameters.length() > 1) {
                mongo.tracing = "enable".equals(parameters.at(1).asString());
                mongo.traceLimit = parameters.at(2).asInt(0);
            }

            if (mongo.tracing) {
                output.line("Tracing is enabled...");
                output.blankLine();
                for (Map.Entry<String, Tuple<String, String>> e : mongo.traceData.entrySet()) {
                    output.line(e.getKey());
                    output.separator();
                    output.blankLine();
                    output.line("Query");
                    output.line(e.getValue().getFirst());
                    output.blankLine();
                    output.line("Report");
                    output.line(e.getValue().getSecond());
                    output.blankLine();
                    output.blankLine();
                }
            } else {
                output.line("Tracing is disabled...");
            }

            mongo.traceData.clear();
            return;
        }

        output.line("Control tracing with 'mongo tracing <enable/disable> <limit>'.");
        output.line("Where limit specifies the minimal query duration in milliseconds to be eligible for tracing.");
        output.line("Call 'mongo tracing' to view and reset reace data.");
        output.blankLine();
        output.line("Mongo DB Statistics");
        output.separator();
        output.line(mongo.db().runCommand(Document.parse("{ dbStats: 1, scale: 1 }")).toString());
    }

    @Override
    public String getDescription() {
        return "Reports the state of the attached Mongo DB and enables / disables tracing";
    }

    @Nonnull
    @Override
    public String getName() {
        return "mongo";
    }
}
