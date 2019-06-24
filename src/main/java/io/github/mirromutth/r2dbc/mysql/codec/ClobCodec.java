/*
 * Copyright 2018-2019 the original author or authors.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package io.github.mirromutth.r2dbc.mysql.codec;

import io.github.mirromutth.r2dbc.mysql.codec.lob.ScalarClob;
import io.github.mirromutth.r2dbc.mysql.collation.CharCollation;
import io.github.mirromutth.r2dbc.mysql.constant.DataType;
import io.github.mirromutth.r2dbc.mysql.internal.MySqlSession;
import io.github.mirromutth.r2dbc.mysql.message.FieldValue;
import io.github.mirromutth.r2dbc.mysql.message.LargeFieldValue;
import io.github.mirromutth.r2dbc.mysql.message.NormalFieldValue;
import io.github.mirromutth.r2dbc.mysql.message.ParameterValue;
import io.github.mirromutth.r2dbc.mysql.message.client.ParameterWriter;
import io.r2dbc.spi.Clob;
import org.reactivestreams.Publisher;
import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;

import java.lang.reflect.Type;
import java.util.Objects;
import java.util.concurrent.atomic.AtomicReferenceFieldUpdater;

/**
 * Codec for {@link Clob}.
 * <p>
 * Note: {@link Clob} will be written by {@code ParameterWriter} rather than {@link #encode}.
 */
final class ClobCodec implements Codec<Clob, FieldValue, Class<? super Clob>> {

    static final ClobCodec INSTANCE = new ClobCodec();

    private ClobCodec() {
    }

    @Override
    public Clob decodeText(FieldValue value, FieldInformation info, Class<? super Clob> target, MySqlSession session) {
        return decodeBoth(value, info, session);
    }

    @Override
    public Clob decodeBinary(FieldValue value, FieldInformation info, Class<? super Clob> target, MySqlSession session) {
        return decodeBoth(value, info, session);
    }

    @Override
    public boolean canDecode(FieldValue value, FieldInformation info, Type target) {
        if (info.getCollationId() == CharCollation.BINARY_ID || !(target instanceof Class<?>)) {
            return false;
        }

        DataType type = info.getType();
        if (!TypeConditions.isLob(type) && DataType.JSON != type) {
            return false;
        }

        if (!(value instanceof NormalFieldValue) && !(value instanceof LargeFieldValue)) {
            return false;
        }

        return ((Class<?>) target).isAssignableFrom(Clob.class);
    }

    @Override
    public boolean canEncode(Object value) {
        return value instanceof Clob;
    }

    @Override
    public ParameterValue encode(Object value, MySqlSession session) {
        return new ClobValue((Clob) value, session);
    }

    private static Clob decodeBoth(FieldValue value, FieldInformation info, MySqlSession session) {
        if (value instanceof NormalFieldValue) {
            return ScalarClob.retain(((NormalFieldValue) value).getBuffer(), info.getCollationId(), session.getServerVersion());
        }

        return ScalarClob.retain(((LargeFieldValue) value).getBuffers(), info.getCollationId(), session.getServerVersion());
    }

    private static class ClobValue extends AbstractLobValue {

        private static final AtomicReferenceFieldUpdater<ClobValue, Clob> VALUE_UPDATER =
            AtomicReferenceFieldUpdater.newUpdater(ClobValue.class, Clob.class, "clob");

        private final MySqlSession session;

        private volatile Clob clob;

        private ClobValue(Clob clob, MySqlSession session) {
            this.session = session;
            this.clob = clob;
        }

        @Override
        public Mono<Void> writeTo(ParameterWriter writer) {
            return Mono.defer(() -> {
                Clob clob = VALUE_UPDATER.getAndSet(this, null);

                if (clob == null) {
                    return Mono.error(new IllegalStateException("Clob has written, can not write twice"));
                }

                return Flux.from(clob.stream())
                    .collectList()
                    .doOnNext(sequences -> writer.writeCharSequences(sequences, session.getCollation()))
                    .then();
            });
        }

        @Override
        public boolean equals(Object o) {
            if (this == o) {
                return true;
            }
            if (!(o instanceof ClobValue)) {
                return false;
            }

            ClobValue clobValue = (ClobValue) o;

            return Objects.equals(this.clob, clobValue.clob);
        }

        @Override
        public int hashCode() {
            Clob clob = this.clob;
            return clob == null ? 0 : clob.hashCode();
        }

        @Override
        protected Publisher<Void> getDiscard() {
            Clob clob = VALUE_UPDATER.getAndSet(this, null);
            return clob == null ? null : clob.discard();
        }
    }
}
