{pkgs ? import <nixpkgs> {}}:
pkgs.mkShell {
  buildInputs = with pkgs; [
    # Python and core packages from nixpkgs
    python311
    python311Packages.pip
    python311Packages.virtualenv

    # Data processing
    python311Packages.pandas
    python311Packages.polars
    python311Packages.numpy

    # Database
    python311Packages.sqlalchemy
    python311Packages.pymysql

    # Web scraping
    python311Packages.requests
    python311Packages.beautifulsoup4

    # ML/Stats
    python311Packages.scikit-learn
    python311Packages.scipy
    python311Packages.statsmodels

    # Visualization
    python311Packages.matplotlib

    # MySQL/MariaDB CLI tool
    mariadb
    # Jupyter lab
    python311Packages.jupyterlab
    python311Packages.ipykernel
  ];

  shellHook = ''
    echo "========================================="
    echo "Baseball Project Python Environment"
    echo "========================================="
    echo "Python: $(python --version)"
    echo ""

    # Create virtual environment for packages not in nixpkgs (pybaseball)
    if [ ! -d ".venv" ]; then
      echo "Creating virtual environment for additional packages..."
      python -m venv .venv
      source .venv/bin/activate
      pip install --quiet pybaseball ipykernel
      echo "Virtual environment created with pybaseball and ipykernel"
      echo ""
    else
      source .venv/bin/activate
      echo "Using existing virtual environment"
      echo ""
    fi

    echo "Available commands:"
    echo "  python Code/Data\ Preparation/run_batched_processing.py"
    echo "  mysql -u username -p retrosheet  (mariadb client)"
    echo ""
    echo "To exit: type 'exit'"
    echo "========================================="
  '';
}
