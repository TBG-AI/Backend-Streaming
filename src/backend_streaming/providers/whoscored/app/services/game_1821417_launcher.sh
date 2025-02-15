#!/bin/bash
                # Activate conda environment
                source ~/miniconda3/etc/profile.d/conda.sh
                conda activate streaming

                # Run the processor
                python3 "/Users/ryankang/Projects/TBG/Backend-Streaming/src/backend_streaming/providers/whoscored/app/services/run_scraper.py" "1821417"
                