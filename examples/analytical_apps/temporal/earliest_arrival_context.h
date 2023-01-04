#ifndef EARLIEST_ARRIVAL_CONTEXT_H
#define EARLIEST_ARRIVAL_CONTEXT_H
#include <grape/grape.h>
namespace grape {
template <typename FRAG_T>
class EAContext : public VertexDataContext<FRAG_T, long long> {
 public:
  using oid_t = typename FRAG_T::oid_t;
  using vid_t = typename FRAG_T::vid_t;
  explicit EAContext(const FRAG_T& fragment)
      : VertexDataContext<FRAG_T, long long>(fragment),
        partial_result(this->data()) {}

  void Init(AutoParallelMessageManager<FRAG_T>& messages, oid_t src_id,
            long long start_time, long long end_time) {
    auto& frag = this->fragment();
    auto vertices = frag.Vertices();
    this->source_id = source_id;
    this->start_time = start_time;
    this->end_time = end_time;
    // 定义聚合函数，用于决定什么时候更新信息。
    partial_result.Init(vertices, std::numeric_limits<double>::max(),
                        [](long long* lhs, long long rhs) {
                          if (*lhs > rhs) {
                            *lhs = rhs;
                            return true;
                          } else {
                            return false;
                          }
                        });
    messages.RegisterSyncBuffer(frag, &partial_result,
                                MessageStrategy::kSyncOnOuterVertex);
  }
  void Output(std::ostream& os) override {
    auto& frag = this->fragment();
    auto inner_vertices = frag.InnerVertices();
    for (auto v : inner_vertices) {
      double d = partial_result[v];
      if (d == std::numeric_limits<double>::max()) {
        os << frag.GetId(v) << " 不可达" << std::endl;
      } else {
        os << frag.GetId(v) << " " << std::scientific << d << std::endl;
      }
    }
  }

  oid_t source_id;
  SyncBuffer<typename FRAG_T::vertices_t, long long> partial_result;
  long long start_time;
  long long end_time;
};
}  // namespace grape

#endif