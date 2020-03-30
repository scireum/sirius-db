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
import sirius.kernel.commons.Strings;
import sirius.kernel.commons.Value;
import sirius.kernel.commons.Values;
import sirius.kernel.di.std.Part;
import sirius.kernel.di.std.Register;
import sirius.kernel.health.console.Command;

import javax.annotation.Nonnull;
import java.util.EnumSet;

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

        handleSubCommandOrShowHelp(output, args);
        output.blankLine();
        outputIndexInfos(output);
    }

    private void handleSubCommandOrShowHelp(Output output, Values args) {
        boolean subCommandExecuted = executeSubCommand(output, args);

        if (!subCommandExecuted) {
            output.blankLine();
            output.line("Usage:");
            output.line("es-index create-write-index <Entity>");
            output.line("es-index commit-write-index <Entity>");
            output.line("es-index rollback-write-index <Entity>");
            output.line(
                    "es-index delete-index <Index> \"YES\" (BE VERY CAREFUL - This will delete the index and all its data).");
        }
    }

    private boolean executeSubCommand(Output output, Values args) {
        String subCommand = args.at(0).asString();
        if (Strings.isEmpty(subCommand)) {
            return false;
        }

        if ("create-write-index".equals(subCommand)) {
            elastic.createAndInstallWriteIndex(mixing.getDescriptor(args.at(1).asString()));
            output.line("A new write index has been created...");
            return true;
        }

        if ("commit-write-index".equals(subCommand)) {
            elastic.commitWriteIndex(mixing.getDescriptor(args.at(1).asString()));
            output.line("The write index has been commited...");
            return true;
        }

        if ("rollback-write-index".equals(subCommand)) {
            elastic.rollbackWriteIndex(mixing.getDescriptor(args.at(1).asString()));
            output.line("The write index has been rolled back...");
            return true;
        }

        if ("delete-index".equals(subCommand)) {
            String indexName = args.at(1).asString();
            if (!"YES".equals(args.at(2).asString())) {
                output.apply("Not going to delete %s - append YES as 3rd parameter!", indexName);
                return false;
            }
            elastic.getLowLevelClient().deleteIndex(indexName);
            output.line("The index has been deleted...");
            return true;
        }

        if ("suppress-routing".equals(subCommand)) {
            handleRoutingSuppression(output, args);
            return true;
        }

        output.apply("Unknown sub-command: %s", subCommand);
        return false;
    }

    private void handleRoutingSuppression(Output output, Values args) {
        EntityDescriptor descriptor = mixing.getDescriptor(args.at(1).asString());

        if (args.length() == 2) {
            output.line("Usage:");
            output.line("Output suppressions:");
            output.line("es-index suppress-routing <Entity>");
            output.blankLine();
            output.line("Update suppressions (read and write):");
            output.line("es-index suppress-routing <Entity> <Y/N> <Y/N>");
            output.blankLine();
            output.line("Reset suppressions to default:");
            output.line("es-index suppress-routing <Entity> \"-\"");
            output.blankLine();
        } else if ("-".equals(args.at(2).asString())) {
            elastic.updateRoutingSuppression(descriptor, null);
            output.apply("Default suppression of %s has been restored...", descriptor.getType());
            output.blankLine();
        } else if (args.length() == 4) {
            EnumSet<Elastic.RoutingAccessMode> modes = EnumSet.noneOf(Elastic.RoutingAccessMode.class);
            if ("Y".equals(args.at(3).asString())) {
                modes.add(Elastic.RoutingAccessMode.READ);
            }
            if ("Y".equals(args.at(4).asString())) {
                modes.add(Elastic.RoutingAccessMode.WRITE);
            }
            elastic.updateRoutingSuppression(descriptor, modes);
            output.apply("Suppression of %s has been updated...", descriptor.getType());
            output.blankLine();
        }

        output.apply("Suppression of %s: READ: %s WRITE: %s",
                     descriptor.getType(),
                     elastic.isRoutingSuppressed(descriptor, Elastic.RoutingAccessMode.READ),
                     elastic.isRoutingSuppressed(descriptor, Elastic.RoutingAccessMode.WRITE));
    }

    private void outputIndexInfos(Output output) {
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
