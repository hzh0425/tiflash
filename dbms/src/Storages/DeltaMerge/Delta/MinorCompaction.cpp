// Copyright 2022 PingCAP, Ltd.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

#include <IO/MemoryReadWriteBuffer.h>
#include <Storages/DeltaMerge/ColumnFile/ColumnFileTiny.h>
#include <Storages/DeltaMerge/DMContext.h>
#include <Storages/DeltaMerge/Delta/ColumnFilePersistedSet.h>
#include <Storages/DeltaMerge/Delta/MinorCompaction.h>
#include <Storages/DeltaMerge/WriteBatches.h>
#include <Storages/Page/PageStorage.h>

namespace DB
{
namespace DM
{
MinorCompaction::MinorCompaction(size_t compaction_src_level_, size_t current_compaction_version_)
    : compaction_src_level{compaction_src_level_}
    , current_compaction_version{current_compaction_version_}
{}

// Prepare 的作用就是把一个 task 里面多个 tinyFile 合并成一个大的 tinyFile
void MinorCompaction::prepare(DMContext & context, WriteBatches & wbs, const PageReader & reader)
{
    for (auto & task : tasks)
    {
        if (task.is_trivial_move)
            continue;

        // 已经保证了一个 task 里面的所有 file 的 schema 是一样的, 也即存的列是一样的.
        auto & schema = *(task.to_compact[0]->tryToTinyFile()->getSchema());
        auto compact_columns = schema.cloneEmptyColumns();
        for (auto & file : task.to_compact)
        {
            auto * t_file = file->tryToTinyFile();
            if (unlikely(!t_file))
                throw Exception("The compact candidate is not a ColumnTinyFile", ErrorCodes::LOGICAL_ERROR);

            // We ensure schema of all column files are the same
            // 从 file 里面读取 block
            Block block = t_file->readBlockForMinorCompaction(reader);
            size_t block_rows = block.rows();
            // 取出 block 的每一个列, 放入到 compact_columns 中.
            for (size_t i = 0; i < schema.columns(); ++i)
            {
                compact_columns[i]->insertRangeFrom(*block.getByPosition(i).column, 0, block_rows);
            }
            // 删除这个 tinyFile 对应的 pageId
            wbs.removed_log.delPage(t_file->getDataPageId());
        }
        // 根据 compact_columns 构建一个新的 TinyFile, 同时写入到 pageStorage 中.
        Block compact_block = schema.cloneWithColumns(std::move(compact_columns));
        auto compact_rows = compact_block.rows();
        auto compact_column_file = ColumnFileTiny::writeColumnFile(context, compact_block, 0, compact_rows, wbs, task.to_compact.front()->tryToTinyFile()->getSchema());
        wbs.writeLogAndData();
        task.result = compact_column_file;

        total_compact_files += task.to_compact.size();
        total_compact_rows += compact_rows;
        result_compact_files += 1;
    }
}

bool MinorCompaction::commit(ColumnFilePersistedSetPtr & persisted_file_set, WriteBatches & wbs)
{
    return persisted_file_set->installCompactionResults(shared_from_this(), wbs);
}

String MinorCompaction::info() const
{
    return fmt::format("Compacted {} column files into {} column files, total {} rows.", total_compact_files, result_compact_files, total_compact_rows);
}
} // namespace DM
} // namespace DB
