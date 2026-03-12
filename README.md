Fit_TP28_Onboarding_Project

---

Project Structure

The repository is organised to clearly separate source code, documentation, data, and testing components. This structure helps improve collaboration among team members and makes the project easier to maintain and extend.

```
FIT5120-Onboarding-Team28
│
├── src
│   ├── frontend
│   │   ├── index.html
│   │   ├── css
│   │   │   └── style.css
│   │   └── js
│   │       └── app.js
│   │
│   ├── backend
│   │   ├── api
│   │   │   └── uv_api.py
│   │   └── server.py
│
├── data
│   └── uv_data.csv
│
├── docs
│   ├── report
│   ├── diagrams
│   └── slides
│
├── tests
│
├── README.md
├── requirements.txt 
└── .gitignore
```

Folder Description

src/
Contains all source code for the project, including both frontend and backend components.

frontend/
  Stores all user interface related files. This includes the main webpage structure, styling, and client-side scripts.

  `index.html` – The main entry page of the application.
  `css/` – Contains stylesheets used to design and format the web interface.
  `js/` – Contains JavaScript files responsible for client-side logic and interaction.

backend/
  Contains server-side logic and API integration.

  `api/` – Handles communication with external services such as UV data APIs.
  `server.py` – Main backend service responsible for processing requests and providing data to the frontend.

---

data/
Stores datasets used by the system, such as UV radiation data or other supporting information used for analysis or visualisation.


docs/
Contains project documentation and supporting materials.

report/ – Project reports and written documentation.
diagrams/ – Architecture diagrams, workflow diagrams, or system design illustrations.
slides/ – Presentation slides used during project demonstrations.

---

tests/
Contains test scripts used to validate the functionality of the system and ensure reliability during development.

---

README.md
Provides an overview of the project, including the purpose, setup instructions, and explanation of the repository structure.

---

requirements.txt
Lists the Python dependencies required to run the backend services.

---

.gitignore
Specifies files and directories that should not be tracked by Git, such as temporary files, environment configurations, and local system files.

---



Project Overview
Installation
Running the Project
Tech Stack
Team Members


