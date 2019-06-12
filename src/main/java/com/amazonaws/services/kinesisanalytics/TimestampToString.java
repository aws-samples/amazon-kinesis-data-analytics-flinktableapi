package com.amazonaws.services.kinesisanalytics;

import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.common.typeinfo.Types;
import org.apache.flink.table.functions.ScalarFunction;

import java.sql.Timestamp;

public class TimestampToString extends ScalarFunction {
    public String eval(Timestamp t) {
        return t.toString();
    }

    public TypeInformation<?> getResultType(Class<?>[] signature) {
        return Types.STRING;
    }
}
