import sys
sys.path.append('../source')

import ctu13.pipeline.IngestPipeline as ip


def main():
    ip.IngestPipeline.ingest()

if __name__ == '__main__':
    main()