/*
 * Made with all the love in the world
 * by scireum in Remshalden, Germany
 *
 * Copyright by scireum GmbH
 * http://www.scireum.de - info@scireum.de
 */

package sirius.db.mongo.codec;

import org.bson.BsonReader;
import org.bson.BsonWriter;
import org.bson.codecs.Codec;
import org.bson.codecs.DecoderContext;
import org.bson.codecs.EncoderContext;
import org.bson.types.Decimal128;
import sirius.kernel.commons.Amount;

/**
 * Encodes and decodes Amount objects.
 */
public class AmountCodec implements Codec<Amount> {

    @Override
    public Amount decode(BsonReader reader, DecoderContext decoderContext) {
        return Amount.ofRounded(reader.readDecimal128().bigDecimalValue());
    }

    @Override
    public void encode(BsonWriter writer, Amount value, EncoderContext encoderContext) {
        if (value != null && value.isFilled()) {
            writer.writeDecimal128(new Decimal128(value.getAmount()));
        } else {
            writer.writeNull();
        }
    }

    @Override
    public Class<Amount> getEncoderClass() {
        return Amount.class;
    }
}
