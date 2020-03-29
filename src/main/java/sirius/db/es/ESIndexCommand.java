/*
 * Made with all the love in the world
 * by scireum in Remshalden, Germany
 *
 * Copyright by scireum GmbH
 * http://www.scireum.de - info@scireum.de
 */

package sirius.db.es;

import sirius.db.mixing.EntityDescriptor;
import sirius.db.mixing.Mixing;
import sirius.kernel.commons.Value;
import sirius.kernel.commons.Values;
import sirius.kernel.di.std.Part;
import sirius.kernel.di.std.Register;
import sirius.kernel.health.console.Command;

import javax.annotation.Nonnull;

/**
 * Provides a tool which helps with managing Elasticsearch indices and our mapping of {@link ElasticEntity entities}.
 * <p>
 * This lists all known entities and the indices which are currently in use. Also supports to create, commit
 * and rollback a write index per entity.
 * <p>
 * Finally this permits to entirely wipe and delete an index which should be used VERY CAREFULLY!
 */
@Register
public class ESIndexCommand implements Command {

    @Part
    private Elastic elastic;

    @Part
    private Mixing mixing;

    @Override
    public void execute(Output output, String... arguments) throws Exception {
        Values args = Values.of(arguments);

        if (arguments.length == 0) {
            output.line("Usage:");
            output.line("es-index create-write-index <Entity>");
            output.line("es-index commit-write-index <Entity>");
            output.line("es-index rollback-write-index <Entity>");
            output.line(
                    "es-index delete-index <Index> \"YES\" (BE VERY CAREFUL - This will delete the index and all its data).");
        } else {
            handleInnerCommand(output, args);
        }

        output.blankLine();
        output.apply("%-20s %-40s %-40s", "ENTITY", "READ INDEX", "WRITE INDEX");
        output.separator();
        for (EntityDescriptor descriptor : mixing.getDescriptors()) {
            if (ElasticEntity.class.isAssignableFrom(descriptor.getType())) {
                String readAlias = elastic.determineReadAlias(descriptor);
                String readIndex = elastic.getLowLevelClient().resolveIndexForAlias(readAlias).orElse("-");
                String writeIndex = Value.of(elastic.determineWriteAlias(descriptor)).ignore(readAlias).asString("-");
                output.apply("%-20s %-40s %-40s", Mixing.getNameForType(descriptor.getType()), readIndex, writeIndex);
            }
        }
        output.separator();
    }

    private void handleInnerCommand(Output output, Values args) {
        if ("create-write-index".equals(args.at(0).asString())) {
            elastic.createAndInstallWriteIndex(mixing.getDescriptor(args.at(1).asString()));
            output.line("A new write index has been created...");
            return;
        }

        if ("commit-write-index".equals(args.at(0).asString())) {
            elastic.commitWriteIndex(mixing.getDescriptor(args.at(1).asString()));
            output.line("The write index has been commited...");
            return;
        }

        if ("rollback-write-index".equals(args.at(0).asString())) {
            elastic.rollbackWriteIndex(mixing.getDescriptor(args.at(1).asString()));
            output.line("The write index has been rolled back...");
            return;
        }

        if ("delete-index".equals(args.at(0).asString())) {
            String indexName = args.at(1).asString();
            if (!"YES".equals(args.at(2).asString())) {
                output.apply("Not going to delete %s - append YES as 3rd parameter!", indexName);
            }
            elastic.getLowLevelClient().deleteIndex(indexName);
            output.line("The index has been deleted...");
        }
    }

    @Override
    public String getDescription() {
        return "Lists or modifies the read and write index being used per Elastic entity";
    }

    @Nonnull
    @Override
    public String getName() {
        return "es-index";
    }
}
