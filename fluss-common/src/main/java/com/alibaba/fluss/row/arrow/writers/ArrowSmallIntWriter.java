/*
 * Copyright (c) 2024 Alibaba Group Holding Ltd.
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

package com.alibaba.fluss.row.arrow.writers;

import com.alibaba.fluss.annotation.Internal;
import com.alibaba.fluss.row.InternalRow;
import com.alibaba.fluss.shaded.arrow.org.apache.arrow.vector.SmallIntVector;

/** {@link ArrowFieldWriter} for SmallInt. */
@Internal
public class ArrowSmallIntWriter extends ArrowFieldWriter<InternalRow> {

    public static ArrowSmallIntWriter forField(SmallIntVector smallIntVector) {
        return new ArrowSmallIntWriter(smallIntVector);
    }

    private ArrowSmallIntWriter(SmallIntVector smallIntVector) {
        super(smallIntVector);
    }

    @Override
    public void doWrite(InternalRow row, int ordinal, boolean handleSafe) {
        SmallIntVector vector = (SmallIntVector) getValueVector();
        if (isNullAt(row, ordinal)) {
            vector.setNull(getCount());
        } else if (handleSafe) {
            vector.setSafe(getCount(), readShort(row, ordinal));
        } else {
            vector.set(getCount(), readShort(row, ordinal));
        }
    }

    public boolean isNullAt(InternalRow row, int ordinal) {
        return row.isNullAt(ordinal);
    }

    public short readShort(InternalRow row, int ordinal) {
        return row.getShort(ordinal);
    }
}