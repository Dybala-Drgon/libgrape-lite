#ifndef EARLIEST_ARRIVAL_CONTEXT_H
#define EARLIEST_ARRIVAL_CONTEXT_H
#include <grape/grape.h>
namespace grape {
template <typename FRAG_T>
class EAContext : public VertexDataContext<FRAG_T, int64_t> {
 public:
  using oid_t = typename FRAG_T::oid_t;
  using vid_t = typename FRAG_T::vid_t;
  explicit EAContext(const FRAG_T& fragment)
      : VertexDataContext<FRAG_T, int64_t>(fragment),
        partial_result(this->data()) {}

  void Init(AutoParallelMessageManager<FRAG_T>& messages, oid_t src_id,
            int64_t start_time, int64_t end_time) {
    auto& frag = this->fragment();
    auto vertices = frag.Vertices();
    this->source_id = src_id;
    this->start_time = start_time;
    this->end_time = end_time;
    // 定义聚合函数，用于决定什么时候更新信息。
    partial_result.Init(vertices, std::numeric_limits<int64_t>::max(),
                        [](int64_t* lhs, int64_t rhs) {
                          if (*lhs > rhs) {
                            *lhs = rhs;
                            return true;
                          } else {
                            return false;
                          }
                        });
    messages.RegisterSyncBuffer(frag, &partial_result,
                                MessageStrategy::kSyncOnOuterVertex);
    std::cout << "source_id " << this->source_id << " start time "
              << this->start_time << " end time " << this->end_time
              << std::endl;
  }
  void Output(std::ostream& os) override {
    auto& frag = this->fragment();
    auto inner_vertices = frag.InnerVertices();
    for (auto v : inner_vertices) {
      int64_t d = partial_result[v];
      if (d == std::numeric_limits<int64_t>::max()) {
        os << frag.GetId(v) << " 不可达" << std::endl;
      } else {
        os << frag.GetId(v) << " " << d << std::endl;
      }
    }
  }

  oid_t source_id;
  SyncBuffer<typename FRAG_T::vertices_t, int64_t> partial_result;
  int64_t start_time;
  int64_t end_time;
};
}  // namespace grape

#endif