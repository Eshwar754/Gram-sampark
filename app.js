import { initializeApp } from "https://www.gstatic.com/firebasejs/10.8.1/firebase-app.js";
import {
    getAuth,
    setPersistence,
    browserLocalPersistence,
    signInWithEmailAndPassword,
    createUserWithEmailAndPassword,
    signOut,
    onAuthStateChanged
} from "https://www.gstatic.com/firebasejs/10.8.1/firebase-auth.js";
import {
    getFirestore,
    collection,
    addDoc,
    setDoc,
    doc,
    onSnapshot,
    query,
    where,
    orderBy,
    serverTimestamp,
    enableIndexedDbPersistence,
    getDoc,
    getDocs,
    deleteDoc
} from "https://www.gstatic.com/firebasejs/10.8.1/firebase-firestore.js";

// TODO: Replace with your actual Firebase project config
const firebaseConfig = {
    apiKey: "AIzaSyD_wuR44KHN1fa_jXpHunL-BhmMGvBDTBM",
    authDomain: "gram-sampark-d5cb8.firebaseapp.com",
    projectId: "gram-sampark-d5cb8",
    storageBucket: "gram-sampark-d5cb8.firebasestorage.app",
    messagingSenderId: "10325008019",
    appId: "1:10325008019:web:26f635ed4b84f7beb57766"
};

const app = initializeApp(firebaseConfig);
const db = getFirestore(app);
const auth = getAuth(app);

// Explicitly set persistence for offline auth survival
setPersistence(auth, browserLocalPersistence)
    .catch((error) => console.error("Persistence error:", error));

// Global State
let currentUser = null;
let userRole = 'user';
let userStatus = 'pending';
let userAssignedVillages = []; // Now stores [{id, name}]
let activeVillage = null; // Now stores {id, name}
let allVillagesCache = []; // Now stores [{id, name}]
let patientUnsubscribe = null;
let villageUnsubscribe = null;
let usersUnsubscribe = null;
let pendingUsersUnsubscribe = null;
let accessRequestsUnsubscribe = null;

// Enable offline persistence
enableIndexedDbPersistence(db).catch((err) => {
    if (err.code == 'failed-precondition') {
        console.warn('Multiple tabs open, persistence can only be enabled in one tab at a a time.');
    } else if (err.code == 'unimplemented') {
        console.warn('The current browser does not support all of the features required to enable persistence');
    }
});

// Auth UI Elements
const loginSection = document.getElementById('login-section');
const pendingSection = document.getElementById('pending-section');
const appContainer = document.getElementById('app-container');
const loginForm = document.getElementById('login-form');
const logoutBtn = document.getElementById('logout-btn');
const userInfo = document.getElementById('user-info');
const loginMsg = document.getElementById('login-msg');

// Admin UI Elements
const panelAdminDashboard = document.getElementById('panel-admin-dashboard');
const panelAdminManage = document.getElementById('panel-admin-manage');
const villageForm = document.getElementById('village-form');
const adminMsg = document.getElementById('admin-msg');
const villageListEl = document.getElementById('village-list');
const pendingUsersList = document.getElementById('pending-users-list');
const villageRequestsList = document.getElementById('village-requests-list');
const approvedUsersList = document.getElementById('approved-users-list');

// User UI Elements
const panelUserDashboard = document.getElementById('panel-user-dashboard');
const panelUserRequest = document.getElementById('panel-user-request');
const activeVillageSelect = document.getElementById('active-village-select');
const userVillageStats = document.getElementById('user-village-stats');
const requestVillageSelect = document.getElementById('request-village-select');
const submitRequestBtn = document.getElementById('submit-request-btn');
const myRequestsList = document.getElementById('my-requests-list');
const requestMsg = document.getElementById('request-msg');

// Shared Panels
const mainNav = document.getElementById('main-nav');
const panelAddData = document.getElementById('panel-add-data');
const panelViewData = document.getElementById('panel-view-data');
const formVillageBanner = document.getElementById('form-village-banner');
const formTargetVillage = document.getElementById('form-target-village');

// Patient UI Elements
const statusIndicator = document.getElementById('status-indicator');
const form = document.getElementById('patient-form');
const patientListEl = document.getElementById('patient-list');
const msgEl = document.getElementById('form-msg');
const searchInput = document.getElementById('search-input');
const clearBtn = document.getElementById('clear-btn');

// Multi-step Form Logic
let currentStep = 1;
const totalSteps = 8;
const nextBtn = document.getElementById('next-btn');
const prevBtn = document.getElementById('prev-btn');
const submitBtn = document.getElementById('submit-btn');

nextBtn.addEventListener('click', () => {
    if (validateStep(currentStep)) {
        changeStep(1);
    }
});
prevBtn.addEventListener('click', () => changeStep(-1));

function changeStep(direction) {
    document.getElementById(`step-${currentStep}`).style.display = 'none';
    document.getElementById(`step${currentStep}-indicator`).classList.remove('active');
    if (direction > 0) document.getElementById(`step${currentStep}-indicator`).classList.add('completed');

    currentStep += direction;

    document.getElementById(`step-${currentStep}`).style.display = 'block';
    document.getElementById(`step${currentStep}-indicator`).classList.add('active');

    prevBtn.style.display = currentStep > 1 ? 'inline-block' : 'none';

    if (currentStep === totalSteps) {
        nextBtn.style.display = 'none';
        submitBtn.style.display = 'inline-block';
    } else {
        nextBtn.style.display = 'inline-block';
        submitBtn.style.display = 'none';
    }
}

function validateStep(step) {
    const stepEl = document.getElementById(`step-${step}`);
    const inputs = stepEl.querySelectorAll('input, select');
    let isValid = true;

    inputs.forEach(input => {
        // Clear previous errors
        input.classList.remove('invalid');
        const existingError = input.parentElement.querySelector('.error-text');
        if (existingError) existingError.remove();

        const val = input.value.trim();
        let errorMsg = '';

        if (input.hasAttribute('required') && !val) {
            errorMsg = 'This field is required.';
        } else if (val) {
            if (input.id === 'mobile' && !/^[0-9]{10}$/.test(val)) {
                errorMsg = 'Mobile must be exactly 10 digits.';
            } else if (input.id === 'pincode' && !/^[0-9]{6}$/.test(val)) {
                errorMsg = 'PIN Code must be 6 digits.';
            } else if (input.type === 'email' && input.id === 'patient_email' && val && !/^[^\s@]+@[^\s@]+\.[^\s@]+$/.test(val)) {
                errorMsg = 'Invalid email format.';
            }
        }

        if (errorMsg) {
            input.classList.add('invalid');
            const errSpan = document.createElement('span');
            errSpan.className = 'error-text';
            errSpan.textContent = errorMsg;
            input.parentElement.appendChild(errSpan);
            isValid = false;
        }
    });

    if (!isValid) {
        // Find first invalid input and scroll to it
        const firstInvalid = stepEl.querySelector('.invalid');
        if (firstInvalid) firstInvalid.focus();
    }
    return isValid;
}

// Dynamic Fields Logic - Cached DOM Elements
const isEmployedSelect = document.getElementById('is_employed');
const sectorSelect = document.getElementById('sector');
const ownsLandSelect = document.getElementById('owns_land');
const childrenSchoolSelect = document.getElementById('children_school');

const empSectorContainer = document.getElementById('employment-sector-container');
const farmerDetailsContainer = document.getElementById('farmer-details-container');
const landDetailsContainer = document.getElementById('land-details-container');
const schoolTypeContainer = document.getElementById('school-type-container');

isEmployedSelect.addEventListener('change', (e) => {
    const employed = e.target.value === 'Yes';
    empSectorContainer.style.display = employed ? 'block' : 'none';
    if (!employed) {
        farmerDetailsContainer.style.display = 'none';
    } else {
        farmerDetailsContainer.style.display = sectorSelect.value === 'Farmer' ? 'block' : 'none';
    }
});

sectorSelect.addEventListener('change', (e) => {
    farmerDetailsContainer.style.display = e.target.value === 'Farmer' ? 'block' : 'none';
});

ownsLandSelect.addEventListener('change', (e) => {
    landDetailsContainer.style.display = e.target.value === 'Yes' ? 'block' : 'none';
});

childrenSchoolSelect.addEventListener('change', (e) => {
    schoolTypeContainer.style.display = e.target.value === 'Yes' ? 'block' : 'none';
});

// Family Members Logic
const addFamilyBtn = document.getElementById('add-family-btn');
const familyContainer = document.getElementById('family-members-container');
let memberCount = 0;

addFamilyBtn.addEventListener('click', addFamilyMember);

function addFamilyMember(data = {}) {
    memberCount++;
    const div = document.createElement('div');
    div.className = 'family-member-card';
    div.id = `member-${memberCount}`;

    div.innerHTML = `
        <button type="button" class="remove-member-btn" onclick="this.parentElement.remove()">X</button>
        <div class="form-group">
            <label>Name</label>
            <input type="text" class="member-name" value="${data.name || ''}" required>
        </div>
        <div class="form-group">
            <label>Relation</label>
            <input type="text" class="member-relation" value="${data.relation || ''}" required>
        </div>
        <div class="form-group">
            <label>Employment</label>
            <input type="text" class="member-employment" value="${data.employment || ''}">
        </div>
        <div class="form-group">
            <label>Gender</label>
            <select class="member-gender" required>
                <option value="">Select</option>
                <option value="Male" ${data.gender === 'Male' ? 'selected' : ''}>Male</option>
                <option value="Female" ${data.gender === 'Female' ? 'selected' : ''}>Female</option>
            </select>
        </div>
        <div class="form-group">
            <label>Marital Status</label>
            <select class="member-marital" required>
                <option value="">Select</option>
                <option value="Single" ${data.marital === 'Single' ? 'selected' : ''}>Single</option>
                <option value="Married" ${data.marital === 'Married' ? 'selected' : ''}>Married</option>
            </select>
        </div>
    `;
    familyContainer.appendChild(div);
}

function getFamilyData() {
    const members = [];
    familyContainer.querySelectorAll('.family-member-card').forEach(card => {
        members.push({
            name: card.querySelector('.member-name').value,
            relation: card.querySelector('.member-relation').value,
            employment: card.querySelector('.member-employment').value,
            gender: card.querySelector('.member-gender').value,
            marital: card.querySelector('.member-marital').value,
        });
    });
    return members;
}

// Keep track of all fetched patients for searching
let allPatients = [];

// Monitor Network Status
function updateOnlineStatus() {
    if (navigator.onLine) {
        statusIndicator.textContent = 'Online & Syncing';
        statusIndicator.className = 'status online';
    } else {
        statusIndicator.textContent = 'Offline (Changes will save locally)';
        statusIndicator.className = 'status offline';
    }
}
window.addEventListener('online', updateOnlineStatus);
window.addEventListener('offline', updateOnlineStatus);
updateOnlineStatus();

// Auth Logic
let isLoginMode = true;
const authTitle = document.getElementById('auth-title');
const authBtn = document.getElementById('auth-btn');
const toggleAuth = document.getElementById('toggle-auth');

toggleAuth.addEventListener('click', (e) => {
    e.preventDefault();
    isLoginMode = !isLoginMode;
    authTitle.textContent = isLoginMode ? 'Login' : 'Register';
    authBtn.textContent = isLoginMode ? 'Login' : 'Sign Up';
    toggleAuth.textContent = isLoginMode ? 'Need an account? Register here' : 'Already have an account? Login here';
});

loginForm.addEventListener('submit', async (e) => {
    e.preventDefault();
    const email = document.getElementById('email').value.trim();
    const password = document.getElementById('password').value;
    try {
        if (isLoginMode) {
            await signInWithEmailAndPassword(auth, email, password);
        } else {
            // Registration
            const userCred = await createUserWithEmailAndPassword(auth, email, password);
            // Auto create unapproved user doc
            await setDoc(doc(db, 'users', userCred.user.uid), {
                email: email,
                role: 'user',
                status: 'pending',
                created_at: serverTimestamp()
            });
        }
        loginForm.reset();
        loginMsg.textContent = '';
    } catch (error) {
        loginMsg.textContent = error.message;
        loginMsg.className = 'error';
    }
});

logoutBtn.addEventListener('click', () => {
    signOut(auth);
});

onAuthStateChanged(auth, async (user) => {
    if (user) {
        currentUser = user;
        userInfo.textContent = user.email;
        logoutBtn.style.display = 'inline-block';
        loginSection.style.display = 'none';
        appContainer.style.display = 'grid';

        // Check Role & Status
        const userDocRef = doc(db, 'users', user.uid);
        const userDoc = await getDoc(userDocRef);

        if (userDoc.exists()) {
            userRole = userDoc.data().role || 'user';
            userStatus = userDoc.data().status || 'pending';
        } else {
            // Fallback for very old users without doc
            userRole = 'user';
            userStatus = 'pending';
            await setDoc(userDocRef, { email: user.email, role: 'user', status: 'pending' });
        }

        if (userRole === 'admin') {
            appContainer.style.display = 'block';
            pendingSection.style.display = 'none';
            setupTabs([
                { id: 'panel-admin-dashboard', title: 'Admin Dashboard' },
                { id: 'panel-admin-manage', title: 'Manage System' },
                { id: 'panel-add-data', title: 'Add Record (Override)' },
                { id: 'panel-view-data', title: 'Global Database' }
            ]);
            adminSetup();
            await fetchAssignedVillages(user);
            setupPatientListener();
        } else if (userStatus === 'approved') {
            appContainer.style.display = 'block';
            pendingSection.style.display = 'none';
            setupTabs([
                { id: 'panel-user-dashboard', title: 'My Dashboard' },
                { id: 'panel-add-data', title: 'Add Data' },
                { id: 'panel-view-data', title: 'View Records' },
                { id: 'panel-user-request', title: 'Request Village Access' }
            ]);
            userSetup();
            await fetchAssignedVillages(user);
            setupPatientListener();
        } else {
            // Pending or Rejected user
            appContainer.style.display = 'none';
            pendingSection.style.display = 'block';

            const reactCont = document.getElementById('reactivation-container');
            const reactBtn = document.getElementById('request-reactivation-btn');
            const pendingStatus = document.getElementById('pending-status-msg');
            const pendingTitle = document.getElementById('pending-title');
            const pendingText = document.getElementById('pending-msg');

            if (userStatus === 'rejected' || userStatus === 'revoked') {
                pendingTitle.textContent = 'Account Access Revoked';
                pendingText.textContent = 'Your access has been revoked by an administrator.';
                reactCont.style.display = 'block';
                reactBtn.onclick = async () => {
                    try {
                        await setDoc(doc(db, 'users', user.uid), { status: 'pending' }, { merge: true });
                        pendingStatus.textContent = 'Re-activation request sent!';
                        pendingStatus.className = 'success';
                        reactCont.style.display = 'none';
                    } catch (e) {
                        pendingStatus.textContent = e.message;
                        pendingStatus.className = 'error';
                    }
                };
            } else {
                pendingTitle.textContent = 'Account Pending Approval';
                pendingText.textContent = 'Your account has been created successfully but is awaiting admin approval.';
                reactCont.style.display = 'none';
            }
        }

    } else {
        currentUser = null;
        userRole = 'user';
        userStatus = 'pending';
        userAssignedVillages = [];
        activeVillage = null;
        userInfo.textContent = '';
        logoutBtn.style.display = 'none';
        loginSection.style.display = 'flex';
        appContainer.style.display = 'none';
        pendingSection.style.display = 'none';

        if (patientUnsubscribe) patientUnsubscribe();
        if (villageUnsubscribe) villageUnsubscribe();
        if (usersUnsubscribe) usersUnsubscribe();
        if (pendingUsersUnsubscribe) pendingUsersUnsubscribe();
        if (accessRequestsUnsubscribe) accessRequestsUnsubscribe();
    }
});

// Tab Navigation Logic
function setupTabs(tabs) {
    mainNav.innerHTML = '';
    const allPanels = document.querySelectorAll('.tab-panel');
    allPanels.forEach(p => p.classList.remove('active'));

    tabs.forEach((tab, index) => {
        const btn = document.createElement('button');
        btn.className = `tab-btn ${index === 0 ? 'active' : ''}`;
        btn.textContent = tab.title;
        btn.onclick = () => {
            document.querySelectorAll('.tab-btn').forEach(b => b.classList.remove('active'));
            allPanels.forEach(p => p.classList.remove('active'));
            btn.classList.add('active');
            document.getElementById(tab.id).classList.add('active');

            // Fix: Reset form when entering data entry tabs to avoid starting at partial/late steps
            if (tab.id === 'panel-add-data') {
                clearForm();
            }

            // Close mobile sidebar after selection
            const sidebarToggle = document.getElementById('sidebar-toggle');
            if (sidebarToggle) sidebarToggle.checked = false;
        };
        mainNav.appendChild(btn);

        if (index === 0) {
            document.getElementById(tab.id).classList.add('active');
        }
    });
}

function setupPatientListener() {
    if (patientUnsubscribe) patientUnsubscribe();

    let q;
    if (userRole === 'admin') {
        // Admins see everything
        q = query(collection(db, "patients"), orderBy("updated_at", "desc"));
    } else {
        // Check if user has any assigned villages to avoid query errors
        if (userAssignedVillages && userAssignedVillages.length > 0) {
            const villageNames = userAssignedVillages.map(v => v.name);
            q = query(
                collection(db, "patients"),
                where("village", "in", villageNames),
                orderBy("updated_at", "desc")
            );
        } else {
            // If no villages are assigned, show nothing (or handle as needed)
            renderPatients([]);
            return;
        }
    }

    patientUnsubscribe = onSnapshot(q, { includeMetadataChanges: true }, (snapshot) => {
        allPatients = [];
        snapshot.forEach((docSnap) => {
            const data = docSnap.data();
            const source = docSnap.metadata.hasPendingWrites ? "Local" : "Server";
            allPatients.push({ id: docSnap.id, source, ...data });
        });
        renderPatients(allPatients);

        if (userRole !== 'admin') {
            updateUserDashboardStats();
        }
    }, (error) => {
        console.error("Patient list error:", error);
        // Note: You may need to create a composite index in Firebase Console 
        // for (village ASC, updated_at DESC)
    });
}

function adminSetup() {
    // Listen for users for approval and approved users
    const pq = query(collection(db, 'users'));
    pendingUsersUnsubscribe = onSnapshot(pq, (snapshot) => {
        document.getElementById('stat-total-users').textContent = snapshot.size;
        pendingUsersList.innerHTML = '';
        approvedUsersList.innerHTML = ''; // Clear both to rebuild
        let pendingApprovalCount = 0;

        snapshot.forEach(docSnap => {
            const data = docSnap.data();
            if (data.role === 'admin') return;

            const div = document.createElement('div');
            div.className = 'list-row';

            const isApproved = data.status === 'approved';
            const approvedOn = data.approved_at ? new Date(data.approved_at.toDate()).toLocaleDateString() : 'N/A';

            div.innerHTML = `
                <div class="col-name">
                    <div class="primary-text">${escapeHTML(data.email)}</div>
                    <div class="secondary-text">User ID: ${docSnap.id}</div>
                </div>
                <div class="col-info">
                    <span class="badge ${isApproved ? 'badge-success' : 'badge-warning'}">${data.status.toUpperCase()}</span>
                </div>
                <div class="col-location">
                    ${isApproved ? `<div id="v-list-${docSnap.id}" style="font-size:0.85rem;">Loading...</div>` : '<span class="secondary-text">Pending</span>'}
                </div>
                <div class="col-actions">
                    ${data.status !== 'approved' ? `<button class="icon-btn" onclick="approveUser('${docSnap.id}')">Approve</button>` : ''}
                    <button class="icon-btn delete" onclick="revokeUser('${docSnap.id}', '${escapeHTML(data.email)}')">Revoke</button>
                </div>
            `;

            if (isApproved) {
                approvedUsersList.appendChild(div);
                fetchUserVillages(data.email, `v-list-${docSnap.id}`);
            } else {
                pendingApprovalCount++;
                pendingUsersList.appendChild(div);
            }
        });

        if (pendingApprovalCount === 0) {
            pendingUsersList.innerHTML = '<div class="empty-state-card">No approvals pending.</div>';
        }
    });

    // Listen to villages
    const vq = query(collection(db, 'villages'), orderBy('name'));
    villageUnsubscribe = onSnapshot(vq, (snapshot) => {
        document.getElementById('stat-total-villages').textContent = snapshot.size;
        villageListEl.innerHTML = '';
        allVillagesCache = [];
        snapshot.forEach(docSnap => {
            const data = docSnap.data();
            const vObj = { id: docSnap.id, name: data.name };
            allVillagesCache.push(vObj);

            const div = document.createElement('div');
            div.className = 'list-row';
            div.innerHTML = `
                <div class="col-name">
                    <div class="primary-text">${escapeHTML(data.name)}</div>
                    <div class="secondary-text">ID: ${docSnap.id}</div>
                </div>
                <div class="col-info" style="grid-column: span 2;">
                    <div class="secondary-text">Assigned Users:</div>
                    <div class="primary-text" style="font-size: 0.85rem;">
                        ${data.assigned_users && data.assigned_users.length > 0 ? data.assigned_users.join(', ') : 'None'}
                    </div>
                </div>
                <div class="col-actions"></div>
            `;
            villageListEl.appendChild(div);
        });

        // Populate Admin dropdown and filters when cache is updated
        setupFormVillageInput();
    });

    // Listen to access requests
    const rq = query(collection(db, 'access_requests'), where('status', '==', 'pending'));
    if (accessRequestsUnsubscribe) accessRequestsUnsubscribe();
    accessRequestsUnsubscribe = onSnapshot(rq, (snapshot) => {
        villageRequestsList.innerHTML = '';
        if (snapshot.empty) villageRequestsList.innerHTML = '<div class="empty-state-card">No pending requests.</div>';
        snapshot.forEach(docSnap => {
            const data = docSnap.data();
            const div = document.createElement('div');
            div.className = 'list-row';
            div.innerHTML = `
                <div class="col-name">
                    <div class="primary-text">${escapeHTML(data.user_email)}</div>
                    <div class="secondary-text">Requested Access</div>
                </div>
                <div class="col-info">
                    <div class="primary-text">${escapeHTML(data.village)}</div>
                </div>
                <div class="col-location"></div>
                <div class="col-actions">
                    <button class="icon-btn" onclick="approveAccess('${docSnap.id}', '${data.user_email}', '${data.village}')">Grant</button>
                    <button class="icon-btn delete" onclick="rejectAccess('${docSnap.id}')">Deny</button>
                </div>
            `;
            villageRequestsList.appendChild(div);
        });
    });

    // Quick listen to total patients for stat
    onSnapshot(query(collection(db, 'patients')), (snap) => {
        document.getElementById('stat-total-patients').textContent = snap.size;
    });
}

// User-specific setup logic
function userSetup() {
    // Listen to global villages for the Request Access dropdown
    const vq = query(collection(db, 'villages'), orderBy('name'));
    if (villageUnsubscribe) villageUnsubscribe();
    villageUnsubscribe = onSnapshot(vq, (snapshot) => {
        requestVillageSelect.innerHTML = '<option value="">Select a village...</option>';
        snapshot.forEach(docSnap => {
            const data = docSnap.data();
            // Check if name exists in assigned list objects
            if (!userAssignedVillages.some(v => v.name === data.name)) {
                const opt = document.createElement('option');
                opt.value = data.name;
                opt.textContent = data.name;
                requestVillageSelect.appendChild(opt);
            }
        });
    });

    // Listen to my requests
    const rq = query(collection(db, 'access_requests'), where('user_email', '==', currentUser.email));
    if (accessRequestsUnsubscribe) accessRequestsUnsubscribe();
    accessRequestsUnsubscribe = onSnapshot(rq, (snapshot) => {
        myRequestsList.innerHTML = '';
        snapshot.forEach(docSnap => {
            const data = docSnap.data();
            const requestedAt = data.created_at ? new Date(data.created_at.toDate()).toLocaleDateString() : 'N/A';
            const approvedAt = data.approved_at ? new Date(data.approved_at.toDate()).toLocaleDateString() : '';

            const div = document.createElement('div');
            div.className = 'request-card';
            div.innerHTML = `
                <div>Village: <strong>${escapeHTML(data.village)}</strong></div>
                <div>Status: <strong class="source-tag" style="padding:2px 6px;">${escapeHTML(data.status).toUpperCase()}</strong></div>
                <div style="font-size:0.8rem; color:#888; margin-top:5px;">
                    Requested: ${requestedAt}
                    ${approvedAt ? ` | Approved: ${approvedAt}` : ''}
                </div>
            `;
            myRequestsList.appendChild(div);
        });
    });

    submitRequestBtn.onclick = async () => {
        const v = requestVillageSelect.value;
        if (!v) return;
        try {
            const existingReqQuery = query(
                collection(db, 'access_requests'),
                where('user_email', '==', currentUser.email),
                where('village', '==', v)
            );
            const existingReqSnap = await getDocs(existingReqQuery);
            if (!existingReqSnap.empty) {
                document.getElementById('request-msg').textContent = 'You have already requested access to this village.';
                document.getElementById('request-msg').className = 'error';
                return;
            }

            await addDoc(collection(db, 'access_requests'), {
                user_email: currentUser.email,
                village: v,
                status: 'pending',
                created_at: serverTimestamp()
            });
            document.getElementById('request-msg').textContent = 'Request submitted!';
            document.getElementById('request-msg').className = 'success';
            setTimeout(() => { document.getElementById('request-msg').textContent = ''; }, 3000);
        } catch (e) {
            document.getElementById('request-msg').textContent = e.message;
            document.getElementById('request-msg').className = 'error';
        }
    };

    activeVillageSelect.onchange = (e) => {
        const selectedName = e.target.value;
        activeVillage = userAssignedVillages.find(v => v.name === selectedName) || null;
        if (activeVillage) {
            formVillageBanner.style.display = 'flex';
            formTargetVillage.textContent = activeVillage.name;
            if (document.getElementById('village')) {
                document.getElementById('village').value = activeVillage.name;
            }
        } else {
            formVillageBanner.style.display = 'none';
            if (document.getElementById('village')) {
                document.getElementById('village').value = userAssignedVillages.length > 0 ? userAssignedVillages[0].name : '';
            }
        }
    };
}

villageForm.addEventListener('submit', async (e) => {
    e.preventDefault();
    const vName = document.getElementById('village_name').value.trim();

    try {
        await addDoc(collection(db, 'villages'), {
            name: vName,
            assigned_users: [], // Array to hold multiple emails
            created_at: serverTimestamp()
        });
        showAdminMsg('Village created successfully!', 'success');
        villageForm.reset();
    } catch (err) {
        showAdminMsg(err.message, 'error');
    }
});

function showAdminMsg(msg, type) {
    adminMsg.textContent = msg;
    adminMsg.className = type;
    setTimeout(() => { adminMsg.textContent = ''; adminMsg.className = ''; }, 3000);
}

// Global scope functions for inline onclick Handlers
// Helper to fetch and render user-specific village tags with granular revoke
async function fetchUserVillages(email, containerId) {
    const container = document.getElementById(containerId);
    const vq = query(collection(db, 'villages'));
    const vSnap = await getDocs(vq);
    let html = '';
    vSnap.forEach(vDoc => {
        const vData = vDoc.data();
        if (vData.assigned_users && vData.assigned_users.includes(email)) {
            html += `<span class="source-tag" style="background:#e3f2fd; color:#1565c0; display:flex; align-items:center; gap:5px;">
                ${escapeHTML(vData.name)}
                <span onclick="revokeVillageFromUser('${escapeHTML(vData.name)}', '${escapeHTML(email)}')" style="cursor:pointer; font-weight:bold; color:#d32f2f;">&times;</span>
            </span>`;
        }
    });
    container.innerHTML = html || 'No specific villages assigned.';
}

window.revokeVillageFromUser = async function (villageName, userEmail) {
    if (!confirm(`Revoke access to ${villageName} for ${userEmail}?`)) return;
    try {
        const vq = query(collection(db, 'villages'), where('name', '==', villageName));
        const snapshot = await getDocs(vq);
        snapshot.forEach(async (docSnap) => {
            const currentUsers = docSnap.data().assigned_users || [];
            const newUsers = currentUsers.filter(e => e !== userEmail);
            await setDoc(doc(db, 'villages', docSnap.id), { assigned_users: newUsers }, { merge: true });
        });
        showAdminMsg(`Access to ${villageName} revoked for ${userEmail}`, 'success');
    } catch (e) {
        showAdminMsg(e.message, 'error');
    }
};

window.approveUser = async function (uid) {
    try {
        await setDoc(doc(db, 'users', uid), {
            status: 'approved',
            approved_at: serverTimestamp()
        }, { merge: true });
        showAdminMsg('User approved!', 'success');
    } catch (e) {
        showAdminMsg(e.message, 'error');
    }
};

window.revokeUser = async function (uid, email) {
    try {
        await setDoc(doc(db, 'users', uid), { status: 'pending' }, { merge: true });

        // 1. Remove from all village assignments 
        const vq = query(collection(db, 'villages'));
        const vSnap = await getDocs(vq);
        vSnap.forEach(async (dSnap) => {
            const assigned = dSnap.data().assigned_users || [];
            if (assigned.includes(email)) {
                const newAssigned = assigned.filter(e => e !== email);
                await setDoc(doc(db, 'villages', dSnap.id), { assigned_users: newAssigned }, { merge: true });
            }
        });

        // 2. Delete all access requests for this user
        const aq = query(collection(db, 'access_requests'), where('user_email', '==', email));
        const aSnap = await getDocs(aq);
        aSnap.forEach(async (aDoc) => {
            await deleteDoc(doc(db, 'access_requests', aDoc.id));
        });

        showAdminMsg('User access revoked and returned to pending limit.', 'success');
    } catch (e) {
        showAdminMsg(e.message, 'error');
    }
};

window.approveAccess = async function (reqId, userEmail, villageName) {
    try {
        // 1. Mark request as approved
        await setDoc(doc(db, 'access_requests', reqId), {
            status: 'approved',
            approved_at: serverTimestamp()
        }, { merge: true });

        // 2. Find village doc and append user
        const vq = query(collection(db, 'villages'), where('name', '==', villageName));
        const snapshot = await getDocs(vq);
        for (const docSnap of snapshot.docs) {
            const currentUsers = docSnap.data().assigned_users || [];
            if (!currentUsers.includes(userEmail)) {
                currentUsers.push(userEmail);
                await setDoc(doc(db, 'villages', docSnap.id), { assigned_users: currentUsers }, { merge: true });
            }
        }
        showAdminMsg('Request approved and village assigned', 'success');
    } catch (e) {
        showAdminMsg(e.message, 'error');
    }
}

window.rejectAccess = async function (reqId) {
    try {
        await setDoc(doc(db, 'access_requests', reqId), { status: 'rejected' }, { merge: true });
        showAdminMsg('Request rejected', 'success');
    } catch (e) {
        showAdminMsg(e.message, 'error');
    }
}

async function fetchAssignedVillages(user) {
    userAssignedVillages = [];
    const q = query(collection(db, 'villages'), where('assigned_users', 'array-contains', user.email));

    // Proper realtime listener for assigned villages to restrict UI dynamically
    onSnapshot(q, (snapshot) => {
        userAssignedVillages = [];
        snapshot.forEach(docSnap => {
            userAssignedVillages.push({
                id: docSnap.id,
                name: docSnap.data().name,
                assigned_at: docSnap.data().assigned_at // Optional if we store it there
            });
        });

        setupFormVillageInput(); // Re-render dropdown 
        setupPatientListener(); // Re-render patient list based on new villages

        if (userRole !== 'admin') {
            activeVillageSelect.innerHTML = '<option value="">Select Village...</option>';
            userVillageStats.innerHTML = '';
            userAssignedVillages.forEach(v => {
                const opt = document.createElement('option');
                opt.value = v.name;
                opt.textContent = v.name;
                activeVillageSelect.appendChild(opt);

                // Add stat card
                const approvedAt = v.assigned_at ? new Date(v.assigned_at.toDate()).toLocaleDateString() : 'N/A';
                const card = document.createElement('div');
                card.className = 'stat-card';
                card.id = `stat-card-${v.name.replace(/\s+/g, '-')}`;
                card.innerHTML = `
                    <h3>${escapeHTML(v.name)}</h3>
                    <div class="value" id="val-${v.name.replace(/\s+/g, '-')}">0</div>
                    <div style="font-size: 0.75rem; color: #888; margin-top: 8px;">Access Granted: ${approvedAt}</div>
                `;
                userVillageStats.appendChild(card);
            });
            // Update counts natively
            updateUserDashboardStats();
        }
    });
}

function updateUserDashboardStats() {
    // Tally up from allPatients list
    const counts = {};
    allPatients.forEach(p => {
        if (p.village) {
            counts[p.village] = (counts[p.village] || 0) + 1;
        }
    });

    userAssignedVillages.forEach(v => {
        const el = document.getElementById(`val-${v.name.replace(/\s+/g, '-')}`);
        if (el) el.textContent = counts[v.name] || 0;
    });
}

function setupFormVillageInput() {
    const container = document.getElementById('village-input-container');
    container.innerHTML = '';

    const filterSelect = document.getElementById('filter-village-select');
    if (filterSelect) {
        filterSelect.innerHTML = '<option value="">All Villages</option>';
    }

    if (userRole === 'admin') {
        const select = document.createElement('select');
        select.id = 'village';
        select.required = true;
        select.innerHTML = '<option value="">Select Village (Override)</option>';
        allVillagesCache.forEach(v => {
            const opt = document.createElement('option');
            opt.value = v.name;
            opt.textContent = v.name;
            select.appendChild(opt);

            if (filterSelect) {
                const fOpt = document.createElement('option');
                fOpt.value = v.name;
                fOpt.textContent = v.name;
                filterSelect.appendChild(fOpt);
            }
        });
        container.appendChild(select);
        select.addEventListener('change', (e) => {
            activeVillage = allVillagesCache.find(v => v.name === e.target.value) || null;
        });
    } else {
        const input = document.createElement('input');
        input.type = 'text';
        input.id = 'village';
        input.readOnly = true;
        input.required = true;
        input.defaultValue = activeVillage ? activeVillage.name : (userAssignedVillages.length > 0 ? userAssignedVillages[0].name : '');
        input.value = input.defaultValue;
        container.appendChild(input);

        userAssignedVillages.forEach(v => {
            if (filterSelect) {
                const fOpt = document.createElement('option');
                fOpt.value = v.name;
                fOpt.textContent = v.name;
                filterSelect.appendChild(fOpt);
            }
        });
    }
}

function renderPatients(patients) {
    patientListEl.innerHTML = '';
    
    const fragment = document.createDocumentFragment();

    // Add header row
    const headerRow = document.createElement('div');
    headerRow.className = 'list-header-row';
    headerRow.innerHTML = `
        <div class="col-name">Patient Details</div>
        <div class="col-info">Demographics</div>
        <div class="col-location">Location</div>
        <div class="col-actions">Actions</div>
    `;
    fragment.appendChild(headerRow);

    patients.forEach(p => {
        const div = document.createElement('div');
        div.className = 'list-row';
        const dateStr = p.updated_at ? new Date(p.client_timestamp || p.updated_at.toDate()).toLocaleDateString() : 'Pending';

        div.innerHTML = `
            <div class="col-name">
                <div class="primary-text">${escapeHTML(p.name)}</div>
                <div class="secondary-text">${p.mobile ? escapeHTML(p.mobile) : 'No Mobile'}</div>
            </div>
            <div class="col-info">
                <div class="primary-text">${escapeHTML(p.gender)}</div>
                <div class="secondary-text">DOB: ${escapeHTML(p.dob)}</div>
            </div>
            <div class="col-location">
                <div class="primary-text">${escapeHTML(p.village)}</div>
                <div class="secondary-text">Income: ₹${escapeHTML(p.annual_income) || '0'}</div>
            </div>
            <div class="col-actions"></div>
        `;

        div.style.cursor = 'pointer';
        div.onclick = (e) => {
            if(!e.target.closest('button')) showReadModal(p);
        };

        const actionContainer = div.querySelector('.col-actions');
        
        // Edit Button with Icon
        const editBtn = document.createElement('button');
        editBtn.className = 'icon-btn';
        editBtn.title = 'Edit Record';
        editBtn.innerHTML = `
            <svg viewBox="0 0 24 24" width="16" height="16" fill="none" stroke="currentColor" stroke-width="2" stroke-linecap="round" stroke-linejoin="round">
                <path d="M11 4H4a2 2 0 0 0-2 2v14a2 2 0 0 0 2 2h14a2 2 0 0 0 2-2v-7"></path>
                <path d="M18.5 2.5a2.121 2.121 0 0 1 3 3L12 15l-4 1 1-4 9.5-9.5z"></path>
            </svg>
        `;
        editBtn.onclick = (e) => { e.stopPropagation(); editPatient(p); };
        actionContainer.appendChild(editBtn);

        // PDF Button with Icon
        const pdfBtn = document.createElement('button');
        pdfBtn.className = 'icon-btn';
        pdfBtn.title = 'Download PDF';
        pdfBtn.innerHTML = `
            <svg viewBox="0 0 24 24" width="16" height="16" fill="none" stroke="currentColor" stroke-width="2" stroke-linecap="round" stroke-linejoin="round">
                <path d="M14 2H6a2 2 0 0 0-2 2v16a2 2 0 0 0 2 2h12a2 2 0 0 0 2-2V8z"></path>
                <polyline points="14 2 14 8 20 8"></polyline>
                <line x1="16" y1="13" x2="8" y2="13"></line>
                <line x1="16" y1="17" x2="8" y2="17"></line>
                <polyline points="10 9 9 9 8 9"></polyline>
            </svg>
        `;
        pdfBtn.onclick = (e) => { e.stopPropagation(); if (window.generatePDF) window.generatePDF(p); };
        actionContainer.appendChild(pdfBtn);

        // Delete Button with Icon
        const delBtn = document.createElement('button');
        delBtn.className = 'icon-btn delete';
        delBtn.title = 'Delete Record';
        delBtn.innerHTML = `
            <svg viewBox="0 0 24 24" width="16" height="16" fill="none" stroke="currentColor" stroke-width="2" stroke-linecap="round" stroke-linejoin="round">
                <polyline points="3 6 5 6 21 6"></polyline>
                <path d="M19 6v14a2 2 0 0 1-2 2H7a2 2 0 0 1-2-2V6m3 0V4a2 2 0 0 1 2-2h4a2 2 0 0 1 2 2v2"></path>
                <line x1="10" y1="11" x2="10" y2="17"></line>
                <line x1="14" y1="11" x2="14" y2="17"></line>
            </svg>
        `;
        delBtn.onclick = (e) => {
            e.stopPropagation();
            if (confirm(`Delete ${p.name}?`)) {
                deleteDoc(doc(db, "patients", p.id)).then(() => applyPatientFilters());
            }
        };
        actionContainer.appendChild(delBtn);

        fragment.appendChild(div);
    });

    patientListEl.appendChild(fragment);
}

function escapeHTML(str) {
    if (str === null || str === undefined) return '';
    return String(str).replace(/[&<>'"]/g,
        tag => ({
            '&': '&amp;',
            '<': '&lt;',
            '>': '&gt;',
            "'": '&#39;',
            '"': '&quot;'
        }[tag] || tag)
    );
}

function deepSanitize(obj) {
    if (Array.isArray(obj)) {
        return obj.map(v => (v && typeof v === 'object') ? deepSanitize(v) : (v === undefined ? "" : v));
    }
    const clean = {};
    Object.keys(obj).forEach(key => {
        const val = obj[key];
        if (val && typeof val === 'object' && !(val instanceof Date)) {
            clean[key] = deepSanitize(val);
        } else {
            clean[key] = val === undefined ? "" : val;
        }
    });
    return clean;
}

// Handle Form Submission
form.addEventListener('submit', async (e) => {
    e.preventDefault();
    if (!validateStep(currentStep)) return;

    // Last-second fallback for village synchronization
    const villageVal = document.getElementById('village').value.trim();
    if (villageVal && !activeVillage) {
        if (userRole === 'admin') {
            activeVillage = allVillagesCache.find(v => v.name === villageVal) || null;
        } else {
            activeVillage = userAssignedVillages.find(v => v.name === villageVal) || null;
        }
    }

    const docId = document.getElementById('docId').value;
    const patientData = {
        name: document.getElementById('name').value.trim(),
        mobile: document.getElementById('mobile').value.trim(),
        email: document.getElementById('patient_email').value.trim(),
        dob: document.getElementById('dob').value,
        caste: document.getElementById('caste').value.trim(),
        gender: document.getElementById('gender').value,
        marital_status: document.getElementById('marital_status').value,

        family_members: getFamilyData(),

        chronic_disease: document.getElementById('chronic_disease').value.trim(),
        vaccination_status: document.getElementById('vaccination_status').value.trim(),
        nearest_healthcare: document.getElementById('nearest_healthcare').value.trim(),

        village: activeVillage ? activeVillage.name : document.getElementById('village').value.trim(),
        village_id: activeVillage ? String(activeVillage.id) : (allVillagesCache.find(v => v.name === document.getElementById('village').value.trim())?.id || ''),
        gram_panchayat: document.getElementById('gram_panchayat').value.trim(),
        taluk: document.getElementById('taluk').value.trim(),
        district: document.getElementById('district').value.trim(),
        state: document.getElementById('state').value.trim(),
        landmark: document.getElementById('landmark').value.trim(),
        pincode: document.getElementById('pincode').value.trim(),

        is_employed: document.getElementById('is_employed').value,
        sector: document.getElementById('sector').value,
        owns_land: document.getElementById('owns_land').value,
        acres: document.getElementById('acres').value,
        sown: document.getElementById('sown').value.trim(),
        expected_yield: document.getElementById('expected_yield').value.trim(),
        livestocks: document.getElementById('livestocks').value.trim(),

        annual_income: document.getElementById('annual_income').value.trim(),
        tax_regime: document.getElementById('tax_regime').value,

        road_access: document.getElementById('road_access').value,
        internet: document.getElementById('internet').value,
        public_transport: document.getElementById('transport').value,
        distance_hospital: document.getElementById('distance_hospital').value || '',
        distance_school: document.getElementById('distance_school').value || '',
        distance_market: document.getElementById('distance_market').value || '',

        highest_qual: document.getElementById('qualification').value.trim(),
        children_school: document.getElementById('children_school').value,
        school_type: document.getElementById('school_type').value,
        school_dropouts: document.getElementById('dropouts').value,

        assigned_by_email: currentUser.email, // Assign to current user
        village_id: activeVillage ? String(activeVillage.id) : (allVillagesCache.find(v => v.name === document.getElementById('village').value.trim())?.id || ''),
        updated_at: serverTimestamp(),
        // Keep a client-side timestamp to perform our manual Last Write Wins check
        client_timestamp: Date.now()
    };

    const sanitizedData = deepSanitize(patientData);

    try {
        if (docId) {
            const patientRef = doc(db, "patients", docId);
            const existingDoc = await getDoc(patientRef);
            if (existingDoc.exists()) {
                const existingData = existingDoc.data();
                if (existingData.client_timestamp && existingData.client_timestamp > patientData.client_timestamp) {
                    showMsg('Cannot update: Server has a newer version of this record.', 'error');
                    return;
                }
            }
            // Fire and forget the save so UI updates immediately even if offline
            setDoc(patientRef, sanitizedData, { merge: true })
                .catch(e => console.error("Background sync error:", e));
            showMsg('Patient record updated successfully! (Syncing in background)', 'success');
        } else {
            // Fire and forget the save so UI updates immediately even if offline
            addDoc(collection(db, "patients"), sanitizedData)
                .catch(e => console.error("Background sync error:", e));
            showMsg('New patient added successfully! (Syncing in background)', 'success');
        }

        clearForm();

        if (document.getElementById('general-modal').style.display === 'flex') {
            closeModal();
        } else {
            // Reset steps back to 1
            currentStep = 1;
            document.querySelectorAll('.step.completed').forEach(el => el.classList.remove('completed'));
            changeStep(0); // Applies initial logic
        }

    } catch (error) {
        console.error("Error writing document: ", error);
        showMsg('Error preparing record. Check console for details.', 'error');
    }
});

function editPatient(p) {
    // Reset steps to 1 before populating
    currentStep = 1;
    document.querySelectorAll('.step.completed').forEach(el => el.classList.remove('completed'));
    document.querySelectorAll('.step-indicator .step').forEach(el => el.classList.remove('active'));
    document.getElementById('step1-indicator').classList.add('active');
    document.querySelectorAll('.form-step').forEach(el => el.style.display = 'none');
    document.getElementById('step-1').style.display = 'block';
    prevBtn.style.display = 'none';
    nextBtn.style.display = 'inline-block';
    submitBtn.style.display = 'none';

    document.getElementById('docId').value = p.id;
    document.getElementById('name').value = p.name || '';
    document.getElementById('mobile').value = p.mobile || '';
    document.getElementById('patient_email').value = p.email || '';
    document.getElementById('dob').value = p.dob || '';
    document.getElementById('caste').value = p.caste || '';
    document.getElementById('gender').value = p.gender || '';
    document.getElementById('marital_status').value = p.marital_status || '';

    // Family Details
    familyContainer.innerHTML = '';
    memberCount = 0;
    if (p.family_members) {
        p.family_members.forEach(m => addFamilyMember(m));
    }

    document.getElementById('chronic_disease').value = p.chronic_disease || '';
    document.getElementById('vaccination_status').value = p.vaccination_status || '';
    document.getElementById('nearest_healthcare').value = p.nearest_healthcare || '';

    document.getElementById('village').value = p.village || '';
    document.getElementById('gram_panchayat').value = p.gram_panchayat || '';
    document.getElementById('taluk').value = p.taluk || '';
    document.getElementById('district').value = p.district || '';
    document.getElementById('state').value = p.state || '';
    document.getElementById('landmark').value = p.landmark || '';
    document.getElementById('pincode').value = p.pincode || '';

    document.getElementById('is_employed').value = p.is_employed || '';
    document.getElementById('is_employed').dispatchEvent(new Event('change'));

    document.getElementById('sector').value = p.sector || '';
    document.getElementById('sector').dispatchEvent(new Event('change'));

    document.getElementById('owns_land').value = p.owns_land || 'No';
    document.getElementById('owns_land').dispatchEvent(new Event('change'));

    document.getElementById('acres').value = p.acres || '';
    document.getElementById('sown').value = p.sown || '';
    document.getElementById('expected_yield').value = p.expected_yield || '';
    document.getElementById('livestocks').value = p.livestocks || '';

    // New taxation demographic fields
    if (document.getElementById('annual_income')) document.getElementById('annual_income').value = p.annual_income || '';
    if (document.getElementById('tax_regime')) document.getElementById('tax_regime').value = p.tax_regime || '';

    document.getElementById('road_access').value = p.road_access || '';
    document.getElementById('internet').value = p.internet || '';
    document.getElementById('transport').value = p.public_transport || '';
    if (document.getElementById('distance_hospital')) document.getElementById('distance_hospital').value = p.distance_hospital || '';
    if (document.getElementById('distance_school')) document.getElementById('distance_school').value = p.distance_school || '';
    if (document.getElementById('distance_market')) document.getElementById('distance_market').value = p.distance_market || '';

    document.getElementById('qualification').value = p.highest_qual || '';
    document.getElementById('children_school').value = p.children_school || '';
    document.getElementById('children_school').dispatchEvent(new Event('change'));
    document.getElementById('school_type').value = p.school_type || '';
    document.getElementById('dropouts').value = p.school_dropouts || '';

    const formSection = document.getElementById('patient-form-section');
    document.getElementById('modal-body-wrapper').appendChild(formSection);
    document.getElementById('general-modal').style.display = 'flex';
    formVillageBanner.style.display = 'none';
}

clearBtn.addEventListener('click', () => {
    clearForm();
    if (document.getElementById('general-modal').style.display === 'flex') {
        closeModal();
    }
});

function closeModal() {
    document.getElementById('general-modal').style.display = 'none';

    // Repark the form back where it belongs
    const formSection = document.getElementById('patient-form-section');
    document.getElementById('panel-add-data').appendChild(formSection);

    // Repark the read-view back where it belongs 
    const readView = document.getElementById('patient-read-view');
    readView.style.display = 'none';
    document.body.appendChild(readView);

    if (activeVillage) {
        formVillageBanner.style.display = 'flex';
    }
    clearForm();
}
document.getElementById('close-modal-btn').addEventListener('click', closeModal);

function showReadModal(p) {
    const wrapper = document.getElementById('modal-body-wrapper');
    const readView = document.getElementById('patient-read-view');
    const grid = document.getElementById('read-grid');

    document.getElementById('read-title').textContent = `${escapeHTML(p.name)}'s Profile`;
    grid.innerHTML = '';

    const addItem = (label, val) => {
        grid.innerHTML += `<div><strong>${label}:</strong><br/>${escapeHTML(val) || '-'}</div>`;
    };

    addItem('Mobile', p.mobile);
    addItem('Email', p.email);
    addItem('DOB', p.dob);
    addItem('Caste', p.caste);
    addItem('Gender', p.gender);
    addItem('Marital Status', p.marital_status);

    // Family length
    addItem('Family Members Count', (p.family_members || []).length);
    if (p.family_members && p.family_members.length > 0) {
        const addHTMLItem = (label, val) => {
            grid.innerHTML += `<div><strong>${label}:</strong><br/>${val || '-'}</div>`;
        };
        const famStr = p.family_members.map(fm => `${escapeHTML(fm.name)} (${escapeHTML(fm.relation)})<br/>${escapeHTML(fm.gender)}, ${escapeHTML(fm.marital)}, ${escapeHTML(fm.employment)}`).join('<br/><br/>');
        addHTMLItem('Family Details', famStr);
    }

    addItem('Chronic Diseases', p.chronic_disease);
    addItem('Vaccinations', p.vaccination_status);
    addItem('Healthcare Access', p.nearest_healthcare);

    addItem('Village', p.village);
    addItem('PIN Code', p.pincode);
    addItem('Gram Panchayat', p.gram_panchayat);
    addItem('Location', `${escapeHTML(p.taluk)}, ${escapeHTML(p.district)}, ${escapeHTML(p.state)}`);

    addItem('Employment', p.is_employed);
    addItem('Sector', p.sector);
    addItem('Annual Income (₹)', p.annual_income);
    addItem('Tax Regime', p.tax_regime);

    addItem('Highest Qual.', p.highest_qual);

    readView.style.display = 'block';
    wrapper.appendChild(readView);
    document.getElementById('general-modal').style.display = 'flex';
}

function clearForm() {
    document.getElementById('docId').value = '';
    form.reset();
    familyContainer.innerHTML = '';
    memberCount = 0;

    // Trigger changes to hide dynamic fields
    document.getElementById('is_employed').dispatchEvent(new Event('change'));
    document.getElementById('sector').dispatchEvent(new Event('change'));
    document.getElementById('owns_land').dispatchEvent(new Event('change'));
    document.getElementById('children_school').dispatchEvent(new Event('change'));

    currentStep = 1;
    document.querySelectorAll('.step.completed').forEach(el => el.classList.remove('completed'));
    document.querySelectorAll('.step-indicator .step').forEach(el => el.classList.remove('active'));
    document.querySelectorAll('.form-step').forEach(el => el.style.display = 'none');
    document.getElementById('step1-indicator').classList.add('active');
    document.getElementById('step-1').style.display = 'block';
    prevBtn.style.display = 'none';
    nextBtn.style.display = 'inline-block';
    submitBtn.style.display = 'none';
}

function showMsg(msg, type) {
    msgEl.textContent = msg;
    msgEl.className = type;
    setTimeout(() => { msgEl.textContent = ''; msgEl.className = ''; }, 3000);
}

// Search functionality
function debounce(func, wait) {
    let timeout;
    return function executedFunction(...args) {
        const later = () => {
            clearTimeout(timeout);
            func(...args);
        };
        clearTimeout(timeout);
        timeout = setTimeout(later, wait);
    };
}

function applyPatientFilters() {
    const term = searchInput.value.toLowerCase();
    const filterSelect = document.getElementById('filter-village-select');
    const villageFilter = filterSelect ? filterSelect.value : '';

    const filtered = allPatients.filter(p => {
        let matchesSearch = true;
        let matchesVillage = true;

        if (term) {
            const nameMatch = p.name ? p.name.toLowerCase().includes(term) : false;
            const mobileMatch = p.mobile ? String(p.mobile).includes(term) : false;
            const villageMatch = p.village ? p.village.toLowerCase().includes(term) : false;
            matchesSearch = nameMatch || mobileMatch || villageMatch;
        }

        if (villageFilter) {
            matchesVillage = p.village === villageFilter;
        }

        return matchesSearch && matchesVillage;
    });

    renderPatients(filtered);
}

const debouncedApplyPatientFilters = debounce(applyPatientFilters, 250);

searchInput.addEventListener('input', debouncedApplyPatientFilters);

const filterVillageSelect = document.getElementById('filter-village-select');
if (filterVillageSelect) {
    filterVillageSelect.addEventListener('change', applyPatientFilters);
}

// PDF Generation
window.generatePDF = function (p) {
    if (!window.jspdf) {
        showMsg("PDF Library not loaded.", "error"); return;
    }
    const { jsPDF } = window.jspdf;
    const doc = new jsPDF();

    // Color palette
    const primaryColor = [46, 125, 50]; // #2e7d32
    const lightGray = [240, 240, 240];

    // Header Background
    doc.setFillColor(...primaryColor);
    doc.rect(0, 0, 210, 30, 'F');

    // Header Text
    doc.setTextColor(255, 255, 255);
    doc.setFontSize(22);
    doc.setFont("helvetica", "bold");
    doc.text("GRAM-SAMPARK", 105, 18, { align: "center" });

    doc.setFontSize(10);
    doc.setFont("helvetica", "normal");
    doc.text("Rural Health Records & Demographics Portal", 105, 25, { align: "center" });

    // Reset text color to dark gray
    doc.setTextColor(40, 40, 40);

    let y = 40;

    const checkPageBreak = (neededHeight = 10) => {
        if (y + neededHeight > 280) {
            doc.addPage();
            y = 20;
            return true;
        }
        return false;
    };

    const addSectionHeader = (title) => {
        checkPageBreak(15);
        doc.setFillColor(...lightGray);
        doc.rect(14, y - 5, 182, 8, 'F');
        doc.setFontSize(12);
        doc.setFont("helvetica", "bold");
        doc.setTextColor(...primaryColor);
        doc.text(title.toUpperCase(), 16, y);
        doc.setTextColor(40, 40, 40);
        y += 8;
    };

    const addField = (label, value, x = 14, autoY = true) => {
        const valStr = (value !== undefined && value !== null && value !== '') ? String(value) : 'N/A';
        doc.setFontSize(10);
        doc.setFont("helvetica", "bold");
        doc.text(`${label}:`, x, y);

        // Calculate width while font is still bold
        const labelWidth = doc.getTextWidth(`${label}: `) + 1;

        doc.setFont("helvetica", "normal");

        // Wrap text if needed
        const lines = doc.splitTextToSize(valStr, (x === 14 ? 180 : 90) - labelWidth);
        doc.text(lines, x + labelWidth, y);

        if (autoY) {
            y += (lines.length * 5) + 2;
        }
    };

    // 1. Personal Details (2 Columns)
    addSectionHeader("1. Personal Details");
    checkPageBreak(30);

    addField("Name", p.name, 14, false);
    addField("Mobile", p.mobile, 110, true);

    addField("DOB", p.dob, 14, false);
    addField("Gender", p.gender, 110, true);

    addField("Caste", p.caste, 14, false);
    addField("Marital Status", p.marital_status, 110, true);

    addField("Email", p.email, 14, true);

    y += 2;

    // 1.1 Family Members
    if (p.family_members && p.family_members.length > 0) {
        addSectionHeader("1.1 Family Members");

        // Table Header
        doc.setFontSize(9);
        doc.setFont("helvetica", "bold");
        doc.text("Name", 15, y);
        doc.text("Relation", 65, y);
        doc.text("Gender", 100, y);
        doc.text("Marital", 130, y);
        doc.text("Occupation", 160, y);
        y += 2;
        doc.setDrawColor(200, 200, 200);
        doc.line(14, y, 196, y);
        y += 5;

        doc.setFont("helvetica", "normal");
        p.family_members.forEach((fm) => {
            checkPageBreak(10);
            doc.text(String(fm.name).substring(0, 25), 15, y);
            doc.text(String(fm.relation).substring(0, 15), 65, y);
            doc.text(String(fm.gender), 100, y);
            doc.text(String(fm.marital), 130, y);
            doc.text(String(fm.employment).substring(0, 20), 160, y);
            y += 6;
        });
        y += 2;
    }

    // 2. Health Profile
    addSectionHeader("2. Health Profile");
    addField("Chronic Diseases", p.chronic_disease, 14, true);
    addField("Vaccination Status", p.vaccination_status, 14, true);
    addField("Nearest Facility", p.nearest_healthcare, 14, true);
    y += 2;

    // 3. Location Details
    addSectionHeader("3. Location Details");
    addField("Village / Panchayat", `${p.village} / ${p.gram_panchayat || 'N/A'}`, 14, true);
    addField("Block / Taluk", p.taluk, 14, false);
    addField("District", p.district, 110, true);
    addField("State / PIN", `${p.state} - ${p.pincode}`, 14, true);
    addField("Landmark", p.landmark, 14, true);
    y += 2;

    // 4. Occupation & Economy
    addSectionHeader("4. Occupation & Economy");
    addField("Employment", `${p.is_employed} (${p.sector || 'N/A'})`, 14, false);
    addField("Annual Income", p.annual_income ? `Rs. ${p.annual_income}` : "N/A", 110, true);
    addField("Tax Regime", p.tax_regime, 14, false);
    addField("Owns Land", p.owns_land, 110, true);

    if (p.owns_land === "Yes" && parseFloat(p.acres) > 0) {
        addField("Land Size", `${p.acres} acres`, 14, false);
        addField("Crops Sown", p.sown, 110, true);
        addField("Expected Yield", p.expected_yield, 14, true);
    }
    addField("Livestock", p.livestocks, 14, true);
    y += 2;

    // 5. Infrastructure Accessibility
    addSectionHeader("5. Infrastructure");
    addField("Road Access", p.road_access, 14, false);
    addField("Internet", p.internet, 110, true);
    addField("Public Transport", p.public_transport, 14, true);

    doc.setFontSize(9);
    doc.setFont("helvetica", "italic");
    doc.setTextColor(100, 100, 100);
    doc.text("Distances (approx km):", 14, y);
    doc.setFont("helvetica", "normal");
    doc.setTextColor(40, 40, 40);
    y += 5;
    addField("Hospital", p.distance_hospital, 15, false);
    addField("School", p.distance_school, 75, false);
    addField("Market", p.distance_market, 135, true);
    y += 2;

    // 6. Education
    addSectionHeader("6. Education Details");
    addField("Highest Qualification", p.highest_qual, 14, true);
    addField("Children in School", p.children_school, 14, false);

    if (p.children_school === "Yes") {
        addField("School Type", p.school_type, 110, true);
    } else {
        y += 7; // manually advance if not showing school type
    }
    addField("School Dropouts in Family", p.school_dropouts, 14, true);

    // Footer
    const addFooter = () => {
        const pageCount = doc.internal.getNumberOfPages();
        for (let i = 1; i <= pageCount; i++) {
            doc.setPage(i);
            doc.setDrawColor(200, 200, 200);
            doc.line(14, 282, 196, 282);
            doc.setFontSize(8);
            doc.setTextColor(120, 120, 120);

            const timestamp = new Date().toLocaleString();
            doc.text(`Digitally generated by Gram-Sampark Utility`, 14, 286);
            doc.text(`Authorized by: ${currentUser ? currentUser.email : 'System'} | ${timestamp}`, 14, 290);
            doc.text(`Page ${i} of ${pageCount}`, 196, 290, { align: "right" });
        }
    };
    addFooter();

    doc.save(`GramSampark_${(p.name || 'User').replace(/\s+/g, '_')}.pdf`);
}

// Interactive Cursor Background (Optimized)
const cursorGlow = document.getElementById('cursor-glow');
if (cursorGlow) {
    cursorGlow.style.pointerEvents = 'none';
    cursorGlow.style.zIndex = '-1';
}
let mouseX = window.innerWidth / 2;
let mouseY = window.innerHeight / 2;
let glowX = mouseX;
let glowY = mouseY;
let isTabActive = true;

document.addEventListener('visibilitychange', () => {
    isTabActive = !document.hidden;
});

// Avoid executing heavy logic on every mouse event, just record position
document.addEventListener('mousemove', (e) => {
    mouseX = e.clientX;
    mouseY = e.clientY;
}, { passive: true });

function animateGlow() {
    if (cursorGlow && isTabActive) {
        // Linear interpolation for buttery smooth following
        glowX += (mouseX - glowX) * 0.06;
        glowY += (mouseY - glowY) * 0.06;
        
        // Use translate3d to offload rendering to GPU and avoid layout reflows
        cursorGlow.style.transform = `translate3d(${glowX - (cursorGlow.offsetWidth/2)}px, ${glowY - (cursorGlow.offsetHeight/2)}px, 0)`;
    }
    requestAnimationFrame(animateGlow);
}
animateGlow();
