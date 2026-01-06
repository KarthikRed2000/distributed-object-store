package com.distributed.store.metadata.raft;

import com.distributed.store.common.model.RaftMessage;

public interface MessageHandler {
    void onMessage(RaftMessage message);
}
