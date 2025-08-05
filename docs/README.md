# Documentation Structure

This directory contains the comprehensive documentation for the TFM Air Quality Analysis Platform, organized by component and function.

## Directory Structure

```
docs/
├── index.md                           # Main platform overview
├── getting-started/                   # Platform-wide setup and usage
├── architecture/                      # System architecture documentation
├── etl-pipeline/                      # ETL Pipeline module documentation
│   ├── overview.md                    # ETL module overview
│   ├── getting-started.md             # ETL setup and first run
│   ├── ETL_PIPELINE_DOCUMENTATION.md  # Comprehensive ETL documentation
│   └── components/                    # ETL component details
│       └── extract.md                 # Extract layer documentation
├── ml-pipeline/                       # ML Pipeline module documentation
│   └── overview.md                    # ML module overview
├── web-app/                          # Web Application module documentation
│   └── overview.md                    # Web app overview
├── data-sources/                     # Shared data documentation
├── development/                      # Development and contribution guides
├── deployment/                       # Deployment strategies
└── stylesheets/                     # Custom CSS styling
    └── extra.css
```

## Component Organization

Each major component (ETL Pipeline, ML Pipeline, Web Application) has its own directory with:

- **Overview**: High-level component description and purpose
- **Getting Started**: Component-specific setup and usage
- **Architecture/Implementation**: Detailed technical documentation
- **Components/Submodules**: Documentation for internal components
- **Configuration**: Component-specific configuration guides
- **Testing**: Component testing strategies
- **Troubleshooting**: Common issues and solutions

## File Organization Principles

### Component-Specific Files
- All documentation files are organized within their respective component directories
- No component-specific documentation should be at the repository root
- Each component maintains its own complete documentation set

### Repository-Level Files
- `index.md`: Main platform overview and entry point
- `getting-started/`: Platform-wide setup and installation
- `architecture/`: System-level architectural documentation
- `data-sources/`: Shared data source documentation
- `development/`: Repository-wide development guidelines
- `deployment/`: Platform deployment strategies

### Navigation Structure
The `mkdocs.yml` navigation reflects this organization:
- Repository overview at the top level
- Each component as a major navigation section
- Component-specific pages nested under their sections

## Documentation Standards

### File Naming
- Use lowercase with hyphens: `getting-started.md`
- Be descriptive: `data-quality-report.md` not `report.md`
- Component prefix when needed: `etl-pipeline/configuration.md`

### Content Organization
- Start with purpose and scope
- Include practical examples
- Provide troubleshooting information
- Link to related documentation
- Keep component documentation self-contained

### Cross-References
- Use relative links within the same component
- Use absolute links to other components
- Maintain link integrity when reorganizing

## Building and Serving

### Requirements
Install documentation dependencies:
```bash
pip install -r docs-requirements.txt
```

### Local Development
```bash
# Build documentation
mkdocs build

# Serve locally (http://127.0.0.1:8000)
mkdocs serve

# Serve on different port
mkdocs serve --dev-addr=127.0.0.1:8080
```

### Deployment
```bash
# Deploy to GitHub Pages
mkdocs gh-deploy

# Build for static hosting
mkdocs build
# Serve contents of 'site' directory
```

## Contributing to Documentation

### Adding New Pages
1. Create the markdown file in the appropriate component directory
2. Add the page to the navigation in `mkdocs.yml`
3. Update any cross-references in related pages
4. Test the build locally

### Component Documentation
Each component should maintain:
- Clear overview of purpose and capabilities
- Getting started guide with examples
- Complete technical reference
- Configuration and customization options
- Testing and troubleshooting guides

### Style Guidelines
- Use clear, concise language
- Include code examples where helpful
- Provide practical use cases
- Structure content with clear headings
- Use diagrams for complex concepts (Mermaid supported)

## Current Documentation Status

### ✅ Complete
- Repository overview and main landing page
- ETL Pipeline comprehensive documentation
- ML Pipeline overview
- Web Application overview
- Professional navigation structure

### 🚧 In Progress / Planned
- Architecture detail pages
- Component-specific configuration guides
- API reference documentation
- Development and contribution guidelines
- Deployment guides for different environments

The documentation structure provides a solid foundation that can be extended as the platform grows and evolves.