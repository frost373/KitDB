package top.thinkin.lightd.base;

import lombok.Data;
import lombok.NoArgsConstructor;

import java.io.Serializable;
import java.util.List;

@Data
@NoArgsConstructor
public class DBCommandChunk implements Serializable {
    private static final long serialVersionUID = -1L;
    private DBCommandChunkType type;
    private List<DBCommand> commands;
    private TransactionEntity entity;

    public DBCommandChunk(DBCommandChunkType type, List<DBCommand> commands) {
        this.type = type;
        this.commands = commands;
    }

    public DBCommandChunk(DBCommandChunkType type, TransactionEntity entity) {
        this.type = type;
        this.entity = entity;
    }
}
