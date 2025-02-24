#include <fcntl.h>
#include <sys/mman.h>
#include <sys/stat.h>
#include <unistd.h>
#include <QApplication>
#include <QDebug>
#include <QHBoxLayout>
#include <QLabel>
#include <QMatrix4x4>
#include <QOpenGLBuffer>
#include <QOpenGLContext>
#include <QOpenGLFunctions>
#include <QOpenGLShaderProgram>
#include <QOpenGLVertexArrayObject>
#include <QOpenGLWidget>
#include <QPainter>
#include <QTimer>
#include <QVBoxLayout>
#include <QVector4D>
#include <QWidget>
#include <arbtrie/id_alloc.hpp>
#include <arbtrie/mapped_memory.hpp>
#include <iomanip>
#include <iostream>
#include <sstream>
#include <vector>

// Vertex data for a quad (using triangle strip)
static const float quad_vertices[] = {
    // positions     // texture coords
    -1.0f, -1.0f, 0.0f, 0.0f,  // bottom left
    -1.0f, 1.0f,  0.0f, 1.0f,  // top left
    1.0f,  -1.0f, 1.0f, 0.0f,  // bottom right
    1.0f,  1.0f,  1.0f, 1.0f   // top right
};

class StatsWidget : public QWidget
{
   Q_OBJECT
  public:
   StatsWidget(arbtrie::mapped_memory::allocator_header* header,
               const std::string&                        db_path,
               uint32_t                                  num_segments,
               QWidget*                                  parent = nullptr);
   void setNumSegments(uint32_t num_segments);

  private slots:
   void updateStats();

  private:
   arbtrie::mapped_memory::allocator_header* header_;
   std::string                               db_path_;
   uint32_t                                  num_segments_;
   QLabel*                                   totalSegsLabel_;
   QLabel*                                   freeSpaceLabel_;
   QLabel*                                   recycleLabel_;
   QLabel*                                   activeSessionsLabel_;
   QLabel*                                   readStatsLabel_;
   QLabel*                                   sizeLabel_;
   QLabel*                                   positionsLabel_;
};

class LegendWidget : public QWidget
{
   Q_OBJECT
  public:
   LegendWidget(arbtrie::mapped_memory::allocator_header* header, QWidget* parent = nullptr)
       : QWidget(parent), header_(header)
   {
      setFixedHeight(120);  // Increased height for more items
   }

  protected:
   void paintEvent(QPaintEvent*) override;

  private:
   arbtrie::mapped_memory::allocator_header* header_;
   static const QColor                       sessionColors[];
};

class SegmentGLWidget : public QOpenGLWidget, protected QOpenGLFunctions
{
   Q_OBJECT

  public:
   SegmentGLWidget(arbtrie::mapped_memory::allocator_header* header,
                   const std::string&                        db_path,
                   uint32_t                                  num_segments,
                   QWidget*                                  parent = nullptr);
   void setNumSegments(uint32_t num_segments);

  protected:
   void initializeGL() override;
   void resizeGL(int w, int h) override;
   void paintGL() override;

  private:
   static const QColor                       sessionColors[];
   arbtrie::mapped_memory::allocator_header* header_;
   QOpenGLShaderProgram*                     program_ = nullptr;
   QOpenGLVertexArrayObject                  vao_;
   QOpenGLBuffer                             vbo_;
   std::string                               db_path_;
   uint32_t                                  num_segments_;
};

class IdRegionWidget : public QOpenGLWidget, protected QOpenGLFunctions
{
   Q_OBJECT

  public:
   IdRegionWidget(const std::string& db_path, QWidget* parent = nullptr) : QOpenGLWidget(parent)
   {
      if (parent)
         setParent(parent);
      setMinimumSize(256, 256);

      // Open and map the id_alloc state file
      std::string state_path = db_path + "/ids.state";
      state_fd_              = open(state_path.c_str(), O_RDONLY);
      if (state_fd_ == -1)
      {
         throw std::runtime_error("Failed to open id_alloc state file");
      }

      void* mapped = mmap(nullptr, sizeof(arbtrie::id_alloc::id_alloc_state), PROT_READ, MAP_SHARED,
                          state_fd_, 0);
      if (mapped == MAP_FAILED)
      {
         ::close(state_fd_);
         throw std::runtime_error("Failed to map id_alloc state");
      }

      state_ = static_cast<arbtrie::id_alloc::id_alloc_state*>(mapped);
   }

   ~IdRegionWidget()
   {
      makeCurrent();
      if (program_)
      {
         delete program_;
         program_ = nullptr;
      }
      if (texture_)
      {
         glDeleteTextures(1, &texture_);
         texture_ = 0;
      }
      vbo_.destroy();
      vao_.destroy();
      doneCurrent();

      if (state_)
      {
         munmap(state_, sizeof(arbtrie::id_alloc::id_alloc_state));
         state_ = nullptr;
      }
      if (state_fd_ != -1)
      {
         ::close(state_fd_);
         state_fd_ = -1;
      }
   }

  protected:
   void initializeGL() override
   {
      if (!context())
      {
         qWarning() << "No OpenGL context available";
         return;
      }

      if (!initializeOpenGLFunctions())
      {
         qWarning() << "Could not initialize OpenGL functions";
         return;
      }

      glClearColor(0.0f, 0.0f, 0.0f, 1.0f);

      program_ = new QOpenGLShaderProgram(this);
      if (!program_)
      {
         qWarning() << "Could not create shader program";
         return;
      }

      // Create and compile shaders
      if (!program_->addShaderFromSourceCode(QOpenGLShader::Vertex, R"(
        #version 330 core
        layout (location = 0) in vec2 aPos;
        layout (location = 1) in vec2 aTexCoord;
        out vec2 TexCoord;
        void main() {
            gl_Position = vec4(aPos, 0.0, 1.0);
            TexCoord = aTexCoord;
        })"))
      {
         qWarning() << "Failed to compile vertex shader:" << program_->log();
         return;
      }

      if (!program_->addShaderFromSourceCode(QOpenGLShader::Fragment, R"(
        #version 330 core
        in vec2 TexCoord;
        out vec4 FragColor;
        uniform sampler2D regionTexture;
        void main() {
            float value = texture(regionTexture, TexCoord).r;
            FragColor = vec4(value, value, value, 1.0);
        })"))
      {
         qWarning() << "Failed to compile fragment shader:" << program_->log();
         return;
      }

      if (!program_->link())
      {
         qWarning() << "Shader program failed to link:" << program_->log();
         return;
      }

      // Create VAO and VBO
      vao_.create();
      if (!vao_.isCreated())
      {
         qWarning() << "Failed to create VAO";
         return;
      }
      vao_.bind();

      vbo_.create();
      if (!vbo_.isCreated())
      {
         qWarning() << "Failed to create VBO";
         return;
      }
      vbo_.bind();
      vbo_.allocate(quad_vertices, sizeof(quad_vertices));

      // Position attribute
      program_->enableAttributeArray(0);
      program_->setAttributeBuffer(0, GL_FLOAT, 0, 2, 4 * sizeof(float));
      // TexCoord attribute
      program_->enableAttributeArray(1);
      program_->setAttributeBuffer(1, GL_FLOAT, 2 * sizeof(float), 2, 4 * sizeof(float));

      // Create texture
      glGenTextures(1, &texture_);
      glBindTexture(GL_TEXTURE_2D, texture_);
      glTexParameteri(GL_TEXTURE_2D, GL_TEXTURE_MIN_FILTER, GL_LINEAR);
      glTexParameteri(GL_TEXTURE_2D, GL_TEXTURE_MAG_FILTER, GL_LINEAR);

      // Initialize texture data
      std::vector<float> texData(256 * 256, 0.0f);
      glTexImage2D(GL_TEXTURE_2D, 0, GL_R32F, 256, 256, 0, GL_RED, GL_FLOAT, texData.data());

      vao_.release();
      vbo_.release();
      program_->release();

      // Start update timer after OpenGL initialization
      QTimer* timer = new QTimer(this);
      connect(timer, &QTimer::timeout, this, QOverload<>::of(&QWidget::update));
      timer->start(1000);  // Update every second
   }

   void resizeGL(int w, int h) override { glViewport(0, 0, w, h); }

   void paintGL() override
   {
      if (!state_ || !program_ || !texture_)
         return;

      glClear(GL_COLOR_BUFFER_BIT);

      // Update texture data
      std::vector<float> texData(256 * 256);
      uint32_t           max_use_count = 0;

      // First pass to find max use count for normalization
      for (int i = 0; i < 256 * 256; ++i)
      {
         max_use_count = std::max(max_use_count, state_->regions[i].use_count.load());
      }

      // Second pass to normalize and fill texture data
      for (int i = 0; i < 256 * 256; ++i)
      {
         float use_count = state_->regions[i].use_count.load();
         texData[i]      = max_use_count > 0 ? use_count / max_use_count : 0.0f;
      }

      // Update texture
      glBindTexture(GL_TEXTURE_2D, texture_);
      glTexSubImage2D(GL_TEXTURE_2D, 0, 0, 0, 256, 256, GL_RED, GL_FLOAT, texData.data());

      // Draw quad with texture
      program_->bind();
      vao_.bind();
      glBindTexture(GL_TEXTURE_2D, texture_);
      program_->setUniformValue("regionTexture", 0);
      glDrawArrays(GL_TRIANGLE_STRIP, 0, 4);
      vao_.release();
      program_->release();
   }

  private:
   arbtrie::id_alloc::id_alloc_state* state_    = nullptr;
   int                                state_fd_ = -1;
   QOpenGLShaderProgram*              program_  = nullptr;
   QOpenGLVertexArrayObject           vao_;
   QOpenGLBuffer                      vbo_;
   GLuint                             texture_ = 0;
};

class SegmentVisualizer : public QWidget
{
   Q_OBJECT

  public:
   SegmentVisualizer(const std::string& db_path, QWidget* parent = nullptr);
   ~SegmentVisualizer();

  private:
   bool remapSegsFile();
   void checkAndRemap();

   std::string                               db_path_;
   int                                       header_fd_      = -1;
   int                                       segs_fd_        = -1;
   arbtrie::mapped_memory::allocator_header* header_         = nullptr;
   void*                                     segs_           = nullptr;
   size_t                                    segs_size_      = 0;
   uint32_t                                  num_segments_   = 0;
   SegmentGLWidget*                          glWidget_       = nullptr;
   StatsWidget*                              stats_          = nullptr;
   LegendWidget*                             legend_         = nullptr;
   QTimer*                                   remapTimer_     = nullptr;
   IdRegionWidget*                           idRegionWidget_ = nullptr;
};

// Implementation of StatsWidget
StatsWidget::StatsWidget(arbtrie::mapped_memory::allocator_header* header,
                         const std::string&                        db_path,
                         uint32_t                                  num_segments,
                         QWidget*                                  parent)
    : QWidget(parent), header_(header), db_path_(db_path), num_segments_(num_segments)
{
   auto layout = new QVBoxLayout(this);

   // Create labels for all stats
   totalSegsLabel_      = new QLabel(this);
   freeSpaceLabel_      = new QLabel(this);
   recycleLabel_        = new QLabel(this);
   activeSessionsLabel_ = new QLabel(this);
   readStatsLabel_      = new QLabel(this);
   sizeLabel_           = new QLabel(this);
   positionsLabel_      = new QLabel(this);

   // Add all labels to layout
   layout->addWidget(totalSegsLabel_);
   layout->addWidget(freeSpaceLabel_);
   layout->addWidget(recycleLabel_);
   layout->addWidget(activeSessionsLabel_);
   layout->addWidget(readStatsLabel_);
   layout->addWidget(sizeLabel_);
   layout->addWidget(positionsLabel_);

   QTimer* timer = new QTimer(this);
   connect(timer, &QTimer::timeout, this, &StatsWidget::updateStats);
   timer->start(1000);
   updateStats();
}

void StatsWidget::setNumSegments(uint32_t num_segments)
{
   num_segments_ = num_segments;
   updateStats();
}

void StatsWidget::updateStats()
{
   if (!header_)
      return;

   uint32_t total_segs = num_segments_;
   uint32_t alloc_pos  = header_->alloc_ptr.load();
   uint32_t end_pos    = header_->end_ptr.load();

   // Find minimum read position from all active sessions
   uint32_t min_read_pos = std::numeric_limits<uint32_t>::max();
   for (int i = 0; i < 64; ++i)
   {
      uint64_t session_ptr = header_->session_lock_ptrs[i].load();
      uint32_t read_pos    = session_ptr & 0xFFFFFFFF;  // Lower 32 bits
      if (read_pos != 0)
      {  // Active session
         min_read_pos = std::min(min_read_pos, read_pos);
      }
   }
   if (min_read_pos == std::numeric_limits<uint32_t>::max())
   {
      min_read_pos = alloc_pos;
   }

   // Calculate free space
   double total_size_mb = (total_segs * arbtrie::segment_size) / (1024.0 * 1024.0);
   double used_size_mb  = (alloc_pos * arbtrie::segment_size) / (1024.0 * 1024.0);
   double free_size_mb  = total_size_mb - used_size_mb;
   double free_pct      = (free_size_mb / total_size_mb) * 100.0;

   // Count active sessions
   int active_sessions = 0;
   for (int i = 0; i < 64; ++i)
   {
      uint64_t session_ptr = header_->session_lock_ptrs[i].load();
      if ((session_ptr & 0xFFFFFFFF) != 0)
      {  // If lower 32 bits are non-zero
         active_sessions++;
      }
   }

   // Count segments in recycle queue
   uint32_t recycled_segs = end_pos - alloc_pos;

   // Update labels with proper QString formatting
   totalSegsLabel_->setText(QString("Total Segments: %1").arg(total_segs));
   freeSpaceLabel_->setText(
       QString("Free Space: %1 MB (%2%)").arg(free_size_mb, 0, 'f', 2).arg(free_pct, 0, 'f', 1));
   recycleLabel_->setText(QString("Segments in Recycle Queue: %1").arg(recycled_segs));
   activeSessionsLabel_->setText(QString("Active Sessions: %1").arg(active_sessions));
   readStatsLabel_->setText(QString("Read Position: %1 / %2 / %3 (A->R*: %4, R*->E: %5)")
                                .arg(alloc_pos)
                                .arg(min_read_pos)
                                .arg(end_pos)
                                .arg(min_read_pos - alloc_pos)
                                .arg(end_pos - min_read_pos));
   sizeLabel_->setText(QString("Total Database Size: %1 MB").arg(total_size_mb, 0, 'f', 2));
}

// Implementation of LegendWidget
void LegendWidget::paintEvent(QPaintEvent*)
{
   QPainter painter(this);
   painter.setRenderHint(QPainter::Antialiasing);

   const int boxSize     = 20;
   const int spacing     = 10;
   const int textOffset  = boxSize + spacing;
   const int itemSpacing = 150;  // Horizontal spacing between items
   const int rowSpacing  = 40;   // Vertical spacing between rows

   int x = 10;
   int y = 20;

   // Get active sessions from bitfield
   uint64_t active_sessions = ~header_->free_sessions.load();
   int      num_active      = std::popcount(active_sessions);

   // First row: Active sessions
   while (active_sessions)
   {
      int     bit_pos = std::countr_zero(active_sessions);
      QString label   = QString("Session %1").arg(bit_pos);

      painter.fillRect(x + bit_pos * itemSpacing, y, boxSize, boxSize, sessionColors[bit_pos % 4]);
      painter.drawRect(x + bit_pos * itemSpacing, y, boxSize, boxSize);
      painter.drawText(x + bit_pos * itemSpacing + textOffset, y + boxSize - 5, label);

      active_sessions &= active_sessions - 1;  // Clear lowest set bit
   }

   // Second row
   y += rowSpacing;
   x = 10;

   // Recycled (Orange)
   painter.fillRect(x, y, boxSize, boxSize, QColor(255, 165, 0, 200));
   painter.drawRect(x, y, boxSize, boxSize);
   painter.drawText(x + textOffset, y + boxSize - 5, "In Recycle Queue");

   // Used Space (Green gradient)
   x += itemSpacing * 1.5;
   QLinearGradient gradient(QPointF(x, y), QPointF(x + boxSize * 3, y));
   gradient.setColorAt(0, QColor(0, 50, 0, 200));   // Dark green (full)
   gradient.setColorAt(1, QColor(0, 255, 0, 200));  // Light green (empty)
   painter.fillRect(x, y, boxSize * 3, boxSize, gradient);
   painter.drawRect(x, y, boxSize * 3, boxSize);
   painter.drawText(x, y + boxSize + 15, "Used Space (dark=full, light=empty)");
}

// Define the static member
const QColor LegendWidget::sessionColors[] = {
    QColor(0, 0, 255, 200),    // Blue (Compactor)
    QColor(255, 0, 0, 200),    // Red (Main App)
    QColor(128, 0, 255, 200),  // Purple
    QColor(0, 128, 255, 200),  // Light Blue
};

// Implementation of SegmentGLWidget
SegmentGLWidget::SegmentGLWidget(arbtrie::mapped_memory::allocator_header* header,
                                 const std::string&                        db_path,
                                 uint32_t                                  num_segments,
                                 QWidget*                                  parent)
    : QOpenGLWidget(parent), header_(header), db_path_(db_path), num_segments_(num_segments)
{
   // Only set minimum size in constructor
   setMinimumSize(400, 400);
}

void SegmentGLWidget::setNumSegments(uint32_t num_segments)
{
   num_segments_ = num_segments;
   update();  // Trigger a repaint
}

void SegmentGLWidget::initializeGL()
{
   initializeOpenGLFunctions();
   glClearColor(0.2f, 0.2f, 0.2f, 1.0f);  // Dark gray background
   glEnable(GL_BLEND);
   glBlendFunc(GL_SRC_ALPHA, GL_ONE_MINUS_SRC_ALPHA);

   // Create and compile shaders
   program_ = new QOpenGLShaderProgram(this);
   program_->addShaderFromSourceCode(QOpenGLShader::Vertex, R"(
        #version 330 core
        layout (location = 0) in vec2 aPos;
        layout (location = 1) in vec2 aTexCoord;
        out vec2 TexCoord;
        uniform mat4 transform;
        void main() {
            gl_Position = transform * vec4(aPos, 0.0, 1.0);
            TexCoord = aTexCoord;
        }
    )");
   program_->addShaderFromSourceCode(QOpenGLShader::Fragment, R"(
        #version 330 core
        in vec2 TexCoord;
        out vec4 FragColor;
        uniform vec4 color;
        void main() {
            FragColor = color;
        }
    )");

   if (!program_->link())
   {
      qDebug() << "Shader program failed to link:" << program_->log();
      return;
   }

   // Create VAO and VBO
   vao_.create();
   vao_.bind();

   vbo_.create();
   vbo_.bind();
   vbo_.allocate(quad_vertices, sizeof(quad_vertices));

   // Position attribute
   program_->enableAttributeArray(0);
   program_->setAttributeBuffer(0, GL_FLOAT, 0, 2, 4 * sizeof(float));
   // TexCoord attribute
   program_->enableAttributeArray(1);
   program_->setAttributeBuffer(1, GL_FLOAT, 2 * sizeof(float), 2, 4 * sizeof(float));

   vao_.release();
   vbo_.release();
   program_->release();
}

void SegmentGLWidget::resizeGL(int w, int h)
{
   glViewport(0, 0, w, h);
}

void SegmentGLWidget::paintGL()
{
   if (!header_)
      return;

   glClear(GL_COLOR_BUFFER_BIT);

   // Get current positions
   uint32_t alloc_pos = header_->alloc_ptr.load();
   uint32_t end_pos   = header_->end_ptr.load();

   // Find minimum read position from all active sessions
   uint32_t min_read_pos = std::numeric_limits<uint32_t>::max();
   for (int i = 0; i < 64; ++i)
   {
      uint64_t session_ptr = header_->session_lock_ptrs[i].load();
      uint32_t read_pos    = session_ptr & 0xFFFFFFFF;  // Lower 32 bits
      if (read_pos != 0)
      {  // Active session
         min_read_pos = std::min(min_read_pos, read_pos);
      }
   }
   if (min_read_pos == std::numeric_limits<uint32_t>::max())
   {
      min_read_pos = alloc_pos;
   }

   // Calculate grid dimensions
   int grid_width  = std::ceil(std::sqrt(num_segments_));
   int grid_height = (num_segments_ + grid_width - 1) / grid_width;

   // Calculate cell dimensions with proper padding
   float aspect      = float(width()) / height();
   float cell_width  = 2.0f / grid_width;
   float cell_height = 2.0f / grid_height;

   // Scale cells to maintain aspect ratio and add padding
   float scale_factor = 0.85f;  // 15% padding
   if (aspect > 1.0f)
   {
      cell_width *= scale_factor;
      cell_height *= (scale_factor / aspect);
   }
   else
   {
      cell_width *= (scale_factor * aspect);
      cell_height *= scale_factor;
   }

   program_->bind();
   vao_.bind();

   // Draw segments
   for (uint32_t i = 0; i < num_segments_; ++i)
   {
      const auto& meta  = header_->seg_meta[i];
      auto        state = meta.get_free_state();

      // Skip if completely free and not allocated
      if (state.free_space == arbtrie::segment_size && !state.is_alloc)
      {
         continue;
      }

      // Calculate grid position
      int row = i / grid_width;
      int col = i % grid_width;

      // Calculate normalized device coordinates with proper centering
      float x = -1.0f + cell_width * float(col * 2 + 1);
      float y = 1.0f - cell_height * float(row * 2 + 1);

      // Create transform matrix for position and scale
      QMatrix4x4 transform;
      transform.setToIdentity();
      transform.translate(QVector3D(x, y, 0.0f));
      transform.scale(cell_width, cell_height, 1.0f);

      // Determine color based on segment state
      QVector4D color;
      if (state.is_alloc)
      {
         // Check which session this segment belongs to
         uint64_t active_sessions = ~header_->free_sessions.load();
         int      session_idx     = 0;
         uint64_t sessions        = active_sessions;

         bool found_session = false;
         while (sessions)
         {
            int bit_pos = std::countr_zero(sessions);
            // Check if this segment is the active allocation segment for this session
            if (header_->session_lock_ptrs[bit_pos].load() & (1ULL << 63))  // Check allocation bit
            {
               // This is the active allocation segment for this session
               color         = QVector4D(sessionColors[session_idx % 4].redF(),
                                         sessionColors[session_idx % 4].greenF(),
                                         sessionColors[session_idx % 4].blueF(), 0.8f);
               found_session = true;
               break;
            }
            sessions &= sessions - 1;  // Clear lowest set bit
            session_idx++;
         }

         if (!found_session)
         {
            // If not found to belong to any session, show as inactive
            color = QVector4D(0.5f, 0.5f, 0.5f, 0.8f);
         }
      }
      else
      {
         // Check if segment is in recycle queue by checking if its number appears
         // in the free_seg_buffer between alloc_ptr and end_ptr
         bool in_recycle = false;
         if (end_pos > alloc_pos)
         {  // Normal case
            uint32_t mask = arbtrie::max_segment_count - 1;
            for (uint32_t j = alloc_pos; j < end_pos; ++j)
            {
               if (header_->free_seg_buffer[j & mask] == i)
               {
                  in_recycle = true;
                  break;
               }
            }
         }

         if (in_recycle)
         {
            // Orange for segments in recycle queue
            color = QVector4D(1.0f, 0.65f, 0.0f, 0.8f);
         }
         else
         {
            // Green gradient based on usage
            float usage = 1.0f - (state.free_space / (float)arbtrie::segment_size);
            color       = QVector4D(0.0f, 0.2f + 0.8f * usage, 0.0f, 0.8f);
         }
      }

      // Set uniforms
      program_->setUniformValue("transform", transform);
      program_->setUniformValue("color", color);

      // Draw quad
      glDrawArrays(GL_TRIANGLE_STRIP, 0, 4);
   }

   vao_.release();
   program_->release();
}

// Define the static member
const QColor SegmentGLWidget::sessionColors[] = {
    QColor(0, 0, 255, 200),    // Blue (Compactor)
    QColor(255, 0, 0, 200),    // Red (Main App)
    QColor(128, 0, 255, 200),  // Purple
    QColor(0, 128, 255, 200),  // Light Blue
};

// Implementation of SegmentVisualizer
SegmentVisualizer::SegmentVisualizer(const std::string& db_path, QWidget* parent)
    : QWidget(parent), db_path_(db_path)
{
   setMinimumSize(1200, 600);  // Wider default size
   resize(1600, 800);          // Start with a good size

   // Open the header file in the database directory
   std::string header_path = db_path + "/header";
   header_fd_              = open(header_path.c_str(), O_RDONLY);
   if (header_fd_ == -1)
   {
      throw std::runtime_error("Failed to open database header file");
   }

   void* mapped = mmap(nullptr, sizeof(arbtrie::mapped_memory::allocator_header), PROT_READ,
                       MAP_SHARED, header_fd_, 0);
   if (mapped == MAP_FAILED)
   {
      ::close(header_fd_);
      throw std::runtime_error("Failed to map allocator header");
   }

   header_ = static_cast<arbtrie::mapped_memory::allocator_header*>(mapped);

   // Open and map the segments file
   std::string segs_path = db_path + "/segs";
   segs_fd_              = open(segs_path.c_str(), O_RDONLY);
   if (segs_fd_ == -1)
   {
      munmap(header_, sizeof(arbtrie::mapped_memory::allocator_header));
      ::close(header_fd_);
      throw std::runtime_error("Failed to open segments file");
   }

   if (!remapSegsFile())
   {
      munmap(header_, sizeof(arbtrie::mapped_memory::allocator_header));
      ::close(header_fd_);
      ::close(segs_fd_);
      throw std::runtime_error("Failed to map segments file");
   }

   auto layout = new QVBoxLayout(this);

   stats_ = new StatsWidget(header_, db_path_, num_segments_, this);
   layout->addWidget(stats_);

   legend_ = new LegendWidget(header_, this);
   layout->addWidget(legend_);

   glWidget_ = new SegmentGLWidget(header_, db_path_, num_segments_, this);
   layout->addWidget(glWidget_);

   idRegionWidget_ = new IdRegionWidget(db_path_, this);
   layout->addWidget(idRegionWidget_);
   layout->setStretchFactor(idRegionWidget_, 1);

   layout->setStretchFactor(glWidget_, 1);

   // Create timer to check for file size changes
   remapTimer_ = new QTimer(this);
   connect(remapTimer_, &QTimer::timeout, this, &SegmentVisualizer::checkAndRemap);
   remapTimer_->start(1000);  // Check every second
}

SegmentVisualizer::~SegmentVisualizer()
{
   if (segs_)
   {
      munmap(segs_, segs_size_);
   }
   if (segs_fd_ != -1)
   {
      ::close(segs_fd_);
   }
   if (header_)
   {
      munmap(header_, sizeof(arbtrie::mapped_memory::allocator_header));
   }
   if (header_fd_ != -1)
   {
      ::close(header_fd_);
   }
}

bool SegmentVisualizer::remapSegsFile()
{
   // Get current file size
   struct stat st;
   if (fstat(segs_fd_, &st) == -1)
   {
      return false;
   }

   // If we already have a mapping, unmap it first
   if (segs_)
   {
      munmap(segs_, segs_size_);
      segs_ = nullptr;
   }

   // Store new size
   segs_size_ = st.st_size;

   // Calculate number of segments based on file size
   num_segments_ = segs_size_ / arbtrie::segment_size;

   // Map the file
   segs_ = mmap(nullptr, segs_size_, PROT_READ, MAP_SHARED, segs_fd_, 0);
   if (segs_ == MAP_FAILED)
   {
      segs_ = nullptr;
      return false;
   }

   // Update widgets with new segment count
   if (stats_)
   {
      stats_->setNumSegments(num_segments_);
   }
   if (glWidget_)
   {
      glWidget_->setNumSegments(num_segments_);
   }

   return true;
}

void SegmentVisualizer::checkAndRemap()
{
   // Get current file size
   struct stat st;
   if (fstat(segs_fd_, &st) == -1)
   {
      return;
   }

   // If file size has changed, remap
   if (st.st_size != segs_size_)
   {
      remapSegsFile();
   }
}

int main(int argc, char* argv[])
{
   if (argc != 2)
   {
      std::cerr << "Usage: " << argv[0] << " <database_path>\n";
      return 1;
   }

   QApplication app(argc, argv);

   // Set default surface format for all Qt windows
   QSurfaceFormat format;
   format.setDepthBufferSize(24);
   format.setStencilBufferSize(8);
   format.setVersion(3, 3);
   format.setProfile(QSurfaceFormat::CoreProfile);
   format.setSwapBehavior(QSurfaceFormat::DoubleBuffer);
   QSurfaceFormat::setDefaultFormat(format);

   try
   {
      SegmentVisualizer visualizer(argv[1]);
      visualizer.show();
      return app.exec();
   }
   catch (const std::exception& e)
   {
      std::cerr << "Error: " << e.what() << "\n";
      return 1;
   }
}

#include "segment_visualizer.moc"