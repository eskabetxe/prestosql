/*
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package io.prestosql.elasticsearch.decoders;

import io.prestosql.spi.PrestoException;
import io.prestosql.spi.block.BlockBuilder;
import org.elasticsearch.search.SearchHit;

import java.util.function.Supplier;

import static io.prestosql.spi.StandardErrorCode.TYPE_MISMATCH;
import static io.prestosql.spi.type.RealType.REAL;
import static java.lang.Math.toIntExact;
import static java.lang.String.format;
import static java.util.Objects.requireNonNull;

public class RealDecoder
        implements Decoder
{
    @Override
    public void decode(String path, SearchHit hit, Supplier<Object> getter, BlockBuilder output)
    {
        Object value = getter.get();
        if (value == null) {
            output.appendNull();
        }
        else if (value instanceof Number) {
            REAL.writeLong(output, Float.floatToRawIntBits(((Number) value).floatValue()));
        }
        else {
            throw new PrestoException(TYPE_MISMATCH, format("Expected a numeric value for field %s of type REAL: %s [%s]", path, value, value.getClass().getSimpleName()));
        }
    }

    @Override
    public Object encode(Object value)
    {
        return Float.intBitsToFloat(toIntExact(((Long) value)));
    }
}
