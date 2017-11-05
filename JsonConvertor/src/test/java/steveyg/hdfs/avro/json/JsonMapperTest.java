package steveyg.hdfs.avro.json;

import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Mapper.Context;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.*;
import static org.mockito.Mockito.mock;


class JsonMapperTest {

    private JsonToAvro.JsonMapper mapper;
    private Context context;


    @BeforeEach
    void setUp() {
        mapper = new JsonToAvro.JsonMapper();
        context = mock(Context.class);
    }

    @Test
    void map() {
    }

}