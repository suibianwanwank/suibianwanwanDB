package ccsu;

import ccsu.allTest.AllTest;
import com.ccsu.dm.DataManager;
import com.ccsu.dm.DataManagerImpl;
import com.ccsu.log.RedoLogManager;
import com.ccsu.log.UndoLogManager;
import com.ccsu.server.Executor;
import com.ccsu.tb.TableManager;
import com.ccsu.tb.TableManagerImpl;
import com.ccsu.tm.TransactionManager;
import com.ccsu.tm.TransactionManagerImpl;
import com.ccsu.vm.VersionManager;
import com.ccsu.vm.VersionManagerImpl;
import org.openjdk.jmh.annotations.*;
import org.openjdk.jmh.runner.Runner;
import org.openjdk.jmh.runner.RunnerException;
import org.openjdk.jmh.runner.options.Options;
import org.openjdk.jmh.runner.options.OptionsBuilder;

import java.util.concurrent.TimeUnit;

@BenchmarkMode(Mode.AverageTime)
@State(Scope.Benchmark)
@Fork(1)
@OutputTimeUnit(TimeUnit.SECONDS)
@Warmup(iterations = 2)
@Measurement(iterations = 2)
public class Benchmark2 {






    /**
     * 插入一千条数据的benchmark
     * @param
     * @throws Exception
     */
//    @Benchmark
    public void benchInsert() throws Exception {
        Executor executor=new Executor(0,BenchMarkResource.tableManager);
        for (int i = 2000; i < 3000; i++) {
            byte[] insert = ("insert into table1(a22,b22) values("+i+","+i+");").getBytes();
            executor.execute(insert);
        }
    }

    @Benchmark
    public void benchSelectByIndex() throws Exception {
        Executor executor=new Executor(0,BenchMarkResource2.tableManager);
        for (int i = 100; i < 200; i++) {
            byte[] select = ("select a22,b22 from table1 where a22 > "+45+";").getBytes();
            executor.execute(select);
        }
    }



    public static void main(String[] args) throws Exception {

        Options opt = new OptionsBuilder()
                .include(Benchmark2.class.getSimpleName())
                .build();
        new Runner(opt).run();
    }
}
