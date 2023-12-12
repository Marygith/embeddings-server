package ru.nms.embeddingsserver.jmh;


import lombok.Getter;
import org.openjdk.jmh.annotations.*;

import java.io.IOException;

@State(Scope.Benchmark)
@Getter
public class ExecutionPlan {

    @Param({"10", "100", "10"})
    private int embeddingsAmount;

}
